// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using System.Text;

namespace CrossCloudKit.Basic.DebugPanel;

/// <summary>
/// Renders the debug panel dashboard as a self-contained HTML page
/// with inline CSS and vanilla JavaScript for SSE consumption.
/// Includes browsable entity lists and detail popups.
/// </summary>
public static class DebugPanelHtmlRenderer
{
    /// <summary>
    /// Renders the full HTML page for the current panel state.
    /// All user-provided strings are HTML-encoded to prevent XSS.
    /// </summary>
    public static string Render(DebugPanelState state)
    {
        var sb = new StringBuilder(16384);
        sb.Append("<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">");
        sb.Append("<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">");
        sb.Append("<title>CrossCloudKit Debug Panel</title>");
        sb.Append("<style>");
        AppendCss(sb);
        sb.Append("</style></head><body>");

        // ── Header ────────────────────────────────────────────────────────
        var uptime = DateTime.UtcNow - state.ServerStartedAtUtc;
        sb.Append("<header><h1>CrossCloudKit Debug Panel</h1><div class=\"meta\">");
        sb.Append($"<span>Services: <strong id=\"svc-count\">{state.ServiceCount}</strong></span>");
        sb.Append($"<span>Uptime: {FormatUptime(uptime)}</span>");
        sb.Append("</div></header>");

        // ── Services section ──────────────────────────────────────────────
        sb.Append("<section id=\"services\"><h2>Registered Services</h2><div id=\"svc-cards\" class=\"cards\">");

        var grouped = state.Services.Values
            .GroupBy(s => s.ServiceType)
            .OrderBy(g => g.Key);

        foreach (var group in grouped)
        {
            foreach (var svc in group.OrderBy(s => s.Path))
            {
                state.OperationCountByInstance.TryGetValue(svc.InstanceId, out var opCount);
                var hasBrowse = state.HasDataProvider(svc.InstanceId);
                AppendServiceCard(sb, svc, opCount, hasBrowse);
            }
        }

        if (state.ServiceCount == 0)
            sb.Append("<p class=\"empty\">No services registered.</p>");

        sb.Append("</div></section>");

        // ── Browse panel (hidden by default) ──────────────────────────────
        sb.Append("<section id=\"browse-panel\" class=\"browse-panel hidden\">");
        sb.Append("<div class=\"browse-header\">");
        sb.Append("<h2 id=\"browse-title\">Browse</h2>");
        sb.Append("<button id=\"browse-close\" class=\"btn-close\" title=\"Close\">&times;</button>");
        sb.Append("</div>");
        sb.Append("<div id=\"browse-breadcrumb\" class=\"breadcrumb\"></div>");
        sb.Append("<div id=\"browse-content\" class=\"browse-content\"></div>");
        sb.Append("</section>");

        // ── Operations section ────────────────────────────────────────────
        sb.Append("<section id=\"operations\"><h2>Recent Operations</h2>");
        sb.Append("<div class=\"table-wrap\"><table><thead><tr>");
        sb.Append("<th>Time</th><th>Type</th><th>Operation</th><th>Details</th><th>Duration</th><th>Status</th>");
        sb.Append("</tr></thead><tbody id=\"ops-body\">");

        foreach (var op in state.Operations.Reverse().Take(200))
            AppendOperationRow(sb, op);

        if (!state.Operations.Any())
            sb.Append("<tr><td colspan=\"6\" class=\"empty\">No operations yet.</td></tr>");

        sb.Append("</tbody></table></div></section>");

        // ── Modal overlay for item detail ─────────────────────────────────
        sb.Append("<div id=\"modal-overlay\" class=\"modal-overlay hidden\">");
        sb.Append("<div class=\"modal\">");
        sb.Append("<div class=\"modal-header\">");
        sb.Append("<h3 id=\"modal-title\">Item Detail</h3>");
        sb.Append("<button id=\"modal-close\" class=\"btn-close\">&times;</button>");
        sb.Append("</div>");
        sb.Append("<div id=\"modal-summary\" class=\"modal-summary\"></div>");
        sb.Append("<pre id=\"modal-content\" class=\"modal-content\"></pre>");
        sb.Append("</div></div>");

        // ── SSE + Browse script ───────────────────────────────────────────
        sb.Append("<script>");
        AppendJavaScript(sb);
        sb.Append("</script>");

        sb.Append("</body></html>");
        return sb.ToString();
    }

    // ── CSS ───────────────────────────────────────────────────────────────

    private static void AppendCss(StringBuilder sb)
    {
        sb.Append(@"
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,monospace;background:#1a1a2e;color:#e0e0e0;padding:16px}
header{display:flex;justify-content:space-between;align-items:center;padding:12px 16px;background:#16213e;border-radius:8px;margin-bottom:16px}
header h1{font-size:1.2rem;color:#0ea5e9}
.meta span{margin-left:16px;font-size:0.85rem;color:#94a3b8}
.meta strong{color:#38bdf8}
h2{font-size:1rem;color:#94a3b8;margin-bottom:8px;padding:4px 0;border-bottom:1px solid #334155}
.cards{display:flex;flex-wrap:wrap;gap:12px;margin-bottom:16px}
.card{background:#16213e;border:1px solid #334155;border-radius:8px;padding:12px 16px;min-width:280px;flex:1;max-width:400px;overflow:hidden}
.card .type{font-size:0.75rem;text-transform:uppercase;letter-spacing:1px;color:#0ea5e9;margin-bottom:4px}
.card .path{font-size:0.85rem;color:#e2e8f0;word-break:break-all;overflow-wrap:anywhere}
.card .info{font-size:0.75rem;color:#64748b;margin-top:6px}
.card .ops{color:#22c55e;font-weight:600}
.card .btn-browse{margin-top:8px;padding:4px 12px;background:#0ea5e9;color:#fff;border:none;border-radius:4px;cursor:pointer;font-size:0.75rem;letter-spacing:0.5px}
.card .btn-browse:hover{background:#38bdf8}
.empty{color:#64748b;font-style:italic;padding:12px}

/* Browse panel */
.browse-panel{background:#16213e;border:1px solid #334155;border-radius:8px;padding:16px;margin-bottom:16px}
.browse-panel.hidden{display:none}
.browse-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.browse-header h2{border:none;margin:0;padding:0}
.btn-close{background:none;border:none;color:#94a3b8;font-size:1.5rem;cursor:pointer;padding:0 4px;line-height:1}
.btn-close:hover{color:#e0e0e0}
.breadcrumb{font-size:0.8rem;color:#64748b;margin-bottom:12px}
.breadcrumb a{color:#0ea5e9;cursor:pointer;text-decoration:none}
.breadcrumb a:hover{text-decoration:underline}
.browse-content{max-height:50vh;overflow-y:auto}

/* Container list */
.container-list{display:flex;flex-wrap:wrap;gap:8px}
.container-card{background:#1e293b;border:1px solid #334155;border-radius:6px;padding:10px 14px;cursor:pointer;min-width:200px;flex:1;max-width:300px;transition:border-color 0.15s}
.container-card:hover{border-color:#0ea5e9}
.container-card .c-name{font-size:0.9rem;color:#e2e8f0;font-weight:600}
.container-card .c-count{font-size:0.75rem;color:#64748b;margin-top:2px}
.container-card .c-props{font-size:0.7rem;color:#94a3b8;margin-top:4px}

/* Item list table */
.item-table{width:100%;border-collapse:collapse}
.item-table th{text-align:left;padding:6px 10px;font-size:0.7rem;text-transform:uppercase;color:#94a3b8;border-bottom:1px solid #334155;position:sticky;top:0;background:#16213e}
.item-table td{padding:5px 10px;font-size:0.8rem;border-bottom:1px solid #1e293b}
.item-table tr.clickable{cursor:pointer}
.item-table tr.clickable:hover{background:#1e293b}
.item-prop{font-size:0.75rem;color:#94a3b8}

/* Modal */
.modal-overlay{position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.7);display:flex;align-items:center;justify-content:center;z-index:100}
.modal-overlay.hidden{display:none}
.modal{background:#16213e;border:1px solid #334155;border-radius:8px;width:90%;max-width:800px;max-height:85vh;display:flex;flex-direction:column}
.modal-header{display:flex;justify-content:space-between;align-items:center;padding:12px 16px;border-bottom:1px solid #334155}
.modal-header h3{font-size:1rem;color:#e2e8f0}
.modal-summary{padding:8px 16px;font-size:0.8rem;color:#94a3b8;border-bottom:1px solid #1e293b}
.modal-content{padding:16px;overflow:auto;flex:1;font-size:0.8rem;line-height:1.5;color:#e2e8f0;white-space:pre-wrap;word-break:break-all;background:#0f172a;margin:0;border-radius:0 0 8px 8px}

/* Operations table */
.table-wrap{max-height:40vh;overflow-y:auto;border-radius:8px;border:1px solid #334155}
table{width:100%;border-collapse:collapse}
thead{position:sticky;top:0;background:#16213e;z-index:1}
th{text-align:left;padding:8px 12px;font-size:0.75rem;text-transform:uppercase;letter-spacing:1px;color:#94a3b8;border-bottom:1px solid #334155}
td{padding:6px 12px;font-size:0.82rem;border-bottom:1px solid #1e293b;vertical-align:top}
tr:hover{background:#1e293b}
.ok{color:#22c55e}.err{color:#ef4444}
.dur{color:#f59e0b;font-variant-numeric:tabular-nums}
.loading{color:#94a3b8;font-style:italic;padding:16px}
");
    }

    // ── Card rendering ────────────────────────────────────────────────────

    private static void AppendServiceCard(StringBuilder sb, ServiceRegistration svc, long opCount, bool hasBrowse)
    {
        sb.Append($"<div class=\"card\" data-id=\"{E(svc.InstanceId)}\" data-type=\"{E(svc.ServiceType)}\">");
        sb.Append($"<div class=\"type\">{E(svc.ServiceType)}</div>");
        sb.Append($"<div class=\"path\">{E(svc.Path)}</div>");
        sb.Append($"<div class=\"info\">PID {svc.ProcessId} · {E(svc.MachineName)} · ops: <span class=\"ops\">{opCount}</span></div>");
        if (hasBrowse)
            sb.Append($"<button class=\"btn-browse\" onclick=\"browse('{E(svc.InstanceId)}','{E(svc.ServiceType)}')\">Browse Data</button>");
        sb.Append("</div>");
    }

    // ── Operation row ─────────────────────────────────────────────────────

    private static void AppendOperationRow(StringBuilder sb, OperationEvent op)
    {
        var statusClass = op.Success ? "ok" : "err";
        var statusText = op.Success ? "OK" : "FAIL";
        sb.Append("<tr>");
        sb.Append($"<td>{op.TimestampUtc:HH:mm:ss.fff}</td>");
        sb.Append($"<td>{E(op.ServiceType)}</td>");
        sb.Append($"<td>{E(op.OperationName)}</td>");
        sb.Append($"<td>{E(op.Details)}</td>");
        sb.Append($"<td class=\"dur\">{op.DurationMs}ms</td>");
        sb.Append($"<td class=\"{statusClass}\">{statusText}</td>");
        sb.Append("</tr>");
    }

    // ── JavaScript ────────────────────────────────────────────────────────

    private static void AppendJavaScript(StringBuilder sb)
    {
        sb.Append(@"
(function(){
  /* ── SSE ───────────────────────────────────────────────────────── */
  var es=new EventSource('/api/events');
  var body=document.getElementById('ops-body');
  var cards=document.getElementById('svc-cards');
  var countEl=document.getElementById('svc-count');
  var emptyOps=body.querySelector('.empty');

  es.addEventListener('operation',function(e){
    var op=JSON.parse(e.data);
    if(emptyOps){emptyOps.parentElement.remove();emptyOps=null;}
    var tr=document.createElement('tr');
    var sc=op.Success?'ok':'err';
    var st=op.Success?'OK':'FAIL';
    var t=new Date(op.TimestampUtc);
    var ts=t.toTimeString().split(' ')[0]+'.'+String(t.getMilliseconds()).padStart(3,'0');
    tr.innerHTML='<td>'+esc(ts)+'</td><td>'+esc(op.ServiceType)+'</td><td>'+esc(op.OperationName)+'</td><td>'+esc(op.Details)+'</td><td class=""dur"">'+op.DurationMs+'ms</td><td class=""'+sc+'"">'+st+'</td>';
    body.insertBefore(tr,body.firstChild);
    if(body.children.length>200)body.removeChild(body.lastChild);
    var card=cards.querySelector('[data-id=""'+op.InstanceId+'""]');
    if(card){var s=card.querySelector('.ops');if(s)s.textContent=parseInt(s.textContent||'0')+1;}
  });

  es.addEventListener('service-registered',function(e){
    var s=JSON.parse(e.data);
    var ep=cards.querySelector('.empty');if(ep)ep.remove();
    var existing=cards.querySelector('[data-id=""'+s.InstanceId+'""]');
    if(!existing){
      var d=document.createElement('div');d.className='card';d.setAttribute('data-id',s.InstanceId);d.setAttribute('data-type',s.ServiceType);
      d.innerHTML='<div class=""type"">'+esc(s.ServiceType)+'</div><div class=""path"">'+esc(s.Path)+'</div><div class=""info"">PID '+s.ProcessId+' &middot; '+esc(s.MachineName)+' &middot; ops: <span class=""ops"">0</span></div>';
      // Check if browsable and add button
      fetch('/api/browse/'+s.InstanceId+'/browsable').then(function(r){return r.json();}).then(function(d2){
        if(d2.Browsable){
          var btn=document.createElement('button');btn.className='btn-browse';btn.textContent='Browse Data';
          btn.onclick=function(){browse(s.InstanceId,s.ServiceType);};d.appendChild(btn);
        }
      }).catch(function(){});
      cards.appendChild(d);
    }
    countEl.textContent=cards.querySelectorAll('.card').length;
  });

  es.addEventListener('service-deregistered',function(e){
    var d=JSON.parse(e.data);
    var card=cards.querySelector('[data-id=""'+d.InstanceId+'""]');
    if(card)card.remove();
    countEl.textContent=cards.querySelectorAll('.card').length;
    if(cards.querySelectorAll('.card').length===0){
      var p=document.createElement('p');p.className='empty';p.textContent='No services registered.';cards.appendChild(p);
    }
  });

  function esc(s){if(!s)return '';var d=document.createElement('div');d.appendChild(document.createTextNode(s));return d.innerHTML;}

  /* ── Browse panel ──────────────────────────────────────────────── */
  var browsePanel=document.getElementById('browse-panel');
  var browseTitle=document.getElementById('browse-title');
  var browseCrumb=document.getElementById('browse-breadcrumb');
  var browseContent=document.getElementById('browse-content');
  var browseClose=document.getElementById('browse-close');
  var currentInstanceId=null;
  var currentServiceType=null;

  browseClose.onclick=function(){browsePanel.classList.add('hidden');};

  window.browse=function(instanceId,serviceType){
    currentInstanceId=instanceId;
    currentServiceType=serviceType;
    browseTitle.textContent='Browse '+serviceType;
    browsePanel.classList.remove('hidden');
    showContainers();
  };

  function showContainers(){
    browseCrumb.innerHTML='<a onclick=""showContainers()"">'+esc(currentServiceType)+'</a>';
    browseContent.innerHTML='<div class=""loading"">Loading...</div>';
    fetch('/api/browse/'+currentInstanceId+'/containers')
      .then(function(r){return r.json();})
      .then(function(containers){
        if(!containers||containers.length===0){
          browseContent.innerHTML='<p class=""empty"">No containers found.</p>';
          return;
        }
        var html='<div class=""container-list"">';
        containers.forEach(function(c){
          html+='<div class=""container-card"" onclick=""showItems(\''+escAttr(c.Name)+'\')""><div class=""c-name"">'+esc(c.Name)+'</div>';
          if(c.ItemCount>=0)html+='<div class=""c-count"">'+c.ItemCount+' items</div>';
          if(c.Properties){
            var phtml='';
            for(var k in c.Properties)phtml+=(phtml?'  · ':'')+esc(k)+': '+esc(c.Properties[k]);
            if(phtml)html+='<div class=""c-props"">'+phtml+'</div>';
          }
          html+='</div>';
        });
        html+='</div>';
        browseContent.innerHTML=html;
      })
      .catch(function(err){browseContent.innerHTML='<p class=""empty"">Error: '+esc(err.message)+'</p>';});
  }
  window.showContainers=showContainers;

  window.showItems=function(container){
    browseCrumb.innerHTML='<a onclick=""showContainers()"">'+esc(currentServiceType)+'</a> &raquo; <strong>'+esc(container)+'</strong>';
    browseContent.innerHTML='<div class=""loading"">Loading...</div>';
    fetch('/api/browse/'+currentInstanceId+'/items?container='+encodeURIComponent(container))
      .then(function(r){return r.json();})
      .then(function(items){
        if(!items||items.length===0){
          browseContent.innerHTML='<p class=""empty"">No items in this container.</p>';
          return;
        }
        // Collect all property keys across items for column headers
        var propKeys={};
        items.forEach(function(it){if(it.Properties)for(var k in it.Properties)propKeys[k]=1;});
        var cols=Object.keys(propKeys);

        var html='<table class=""item-table""><thead><tr><th>ID</th>';
        cols.forEach(function(c){html+='<th>'+esc(c)+'</th>';});
        html+='</tr></thead><tbody>';
        items.forEach(function(it){
          var cls=it.HasDetail?'clickable':'';
          var click=it.HasDetail?' onclick=""showDetail(\''+escAttr(container)+'\',\''+escAttr(it.Id)+'\')""':'';
          html+='<tr class=""'+cls+'""'+click+'><td>'+(esc(it.Label||it.Id))+'</td>';
          cols.forEach(function(c){
            html+='<td class=""item-prop"">'+(it.Properties&&it.Properties[c]?esc(it.Properties[c]):'')+'</td>';
          });
          html+='</tr>';
        });
        html+='</tbody></table>';
        browseContent.innerHTML=html;
      })
      .catch(function(err){browseContent.innerHTML='<p class=""empty"">Error: '+esc(err.message)+'</p>';});
  };

  /* ── Detail modal ──────────────────────────────────────────────── */
  var modalOverlay=document.getElementById('modal-overlay');
  var modalTitle=document.getElementById('modal-title');
  var modalSummary=document.getElementById('modal-summary');
  var modalContent=document.getElementById('modal-content');
  var modalClose=document.getElementById('modal-close');

  modalClose.onclick=function(){modalOverlay.classList.add('hidden');};
  modalOverlay.onclick=function(e){if(e.target===modalOverlay)modalOverlay.classList.add('hidden');};
  document.addEventListener('keydown',function(e){if(e.key==='Escape')modalOverlay.classList.add('hidden');});

  window.showDetail=function(container,itemId){
    modalTitle.textContent='Loading...';
    modalSummary.textContent='';
    modalContent.textContent='';
    modalOverlay.classList.remove('hidden');
    fetch('/api/browse/'+currentInstanceId+'/detail?container='+encodeURIComponent(container)+'&id='+encodeURIComponent(itemId))
      .then(function(r){return r.json();})
      .then(function(detail){
        modalTitle.textContent=detail.Id||itemId;
        modalSummary.textContent=detail.Summary||'';
        modalContent.textContent=detail.ContentJson||'No content';
      })
      .catch(function(err){
        modalTitle.textContent='Error';
        modalContent.textContent=err.message;
      });
  };

  function escAttr(s){return (s||'').replace(/\\/g,'\\\\').replace(/'/g,""\\'"");}
})();
");
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    /// <summary>HTML-encode a string to prevent XSS.</summary>
    internal static string E(string? value)
    {
        if (string.IsNullOrEmpty(value)) return "";
        return System.Net.WebUtility.HtmlEncode(value);
    }

    private static string FormatUptime(TimeSpan ts)
    {
        if (ts.TotalHours >= 1) return $"{(int)ts.TotalHours}h {ts.Minutes}m";
        if (ts.TotalMinutes >= 1) return $"{(int)ts.TotalMinutes}m {ts.Seconds}s";
        return $"{(int)ts.TotalSeconds}s";
    }
}
