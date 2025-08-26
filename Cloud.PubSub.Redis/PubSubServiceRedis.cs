// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using Cloud.Interfaces;
using Cloud.Memory.Redis.Common;
using StackExchange.Redis;

namespace Cloud.PubSub.Redis
{
    public class PubSubServiceRedis : RedisCommonFunctionalities, IPubSubService
    {
        /// <summary>
        /// 
        /// <para>PubSubServiceRedis: Parametered Constructor</para>
        /// <para>Note: Redis Pub/Sub service does not keep messages in a permanent queue, therefore if there is not any listener, message will be lost, unlike other Pub/Sub services.</para>
        /// 
        /// <para>Parameters:</para>
        /// <para><paramref name="_RedisHost"/>                     Redis Host (without Port)</para>
        /// <para><paramref name="_RedisPort"/>                     Redis Endpoint Port</para>
        /// <para><paramref name="_RedisUser"/>                     Redis User</para>
        /// <para><paramref name="_RedisPassword"/>                 Redis Server Password</para>
        /// 
        /// </summary>
        public PubSubServiceRedis(
            string _RedisHost,
            int _RedisPort,
            string _RedisUser,
            string _RedisPassword,
            bool _bFailoverMechanismEnabled = true,
            Action<string> _ErrorMessageAction = null) : base("PubSubServiceRedis", _RedisHost, _RedisPort, _RedisUser, _RedisPassword, false, _bFailoverMechanismEnabled, _ErrorMessageAction)
        {
        }

        /// <summary>
        /// 
        /// <para>PubSubServiceRedis: Parametered Constructor</para>
        /// <para>Note: Redis Pub/Sub service does not keep messages in a permanent queue, therefore if there is not any listener, message will be lost, unlike other Pub/Sub services.</para>
        /// 
        /// <para>Parameters:</para>
        /// <para><paramref name="_RedisHost"/>                     Redis Host (without Port)</para>
        /// <para><paramref name="_RedisUser"/>                     Redis User</para>
        /// <para><paramref name="_RedisPort"/>                     Redis Endpoint Port</para>
        /// <para><paramref name="_RedisPassword"/>                 Redis Server Password</para>
        /// <para><paramref name="_RedisSslEnabled"/>               Redis Server SSL Connection Enabled/Disabled</para>
        /// 
        /// </summary>
        public PubSubServiceRedis(
            string _RedisHost,
            int _RedisPort,
            string _RedisUser,
            string _RedisPassword,
            bool _RedisSslEnabled,
            bool _bFailoverMechanismEnabled = true,
            Action<string> _ErrorMessageAction = null) : base("PubSubServiceRedis", _RedisHost, _RedisPort, _RedisUser, _RedisPassword, _RedisSslEnabled, _bFailoverMechanismEnabled, _ErrorMessageAction)
        {
        }


        public bool IsInitialized => throw new NotImplementedException();

        public async Task<bool> DeleteTopicAsync(string topic, Action<string>? errorMessageAction = null, CancellationToken cancellationToken = default)
        {
            if (topic != null && topic.Length > 0)
            {
                try
                {
                    await RedisConnection.GetSubscriber().UnsubscribeAsync(topic, null);
                }
                catch (Exception e)
                {
                    if (bFailoverMechanismEnabled && (e is RedisException || e is TimeoutException))
                    {
                        OnFailoverDetected(errorMessageAction);
                        await DeleteTopicAsync(topic, errorMessageAction);
                    }
                    else
                    {
                        errorMessageAction?.Invoke($"PubSubServiceRedis->DeleteTopicGlobally: {e.Message}, Trace: {e.StackTrace}");
                    }
                }
            }
            return true;
        }

        public async Task<bool> EnsureTopicExistsAsync(string topic, Action<string>? errorMessageAction = null, CancellationToken cancellationToken = default)
        {
            return true;
        }

        public async Task<bool> PublishAsync(string topic, string message, Action<string>? errorMessageAction = null, CancellationToken cancellationToken = default)
        {
            if (topic != null && topic.Length > 0
                && message != null && message.Length > 0)
            {
                FailoverCheck();
                try
                {
                    using (var CreatedTask = RedisConnection.GetDatabase().PublishAsync(topic, message))
                    {
                        CreatedTask.Wait();
                    }
                }
                catch (Exception e)
                {
                    if (bFailoverMechanismEnabled && (e is RedisException || e is TimeoutException))
                    {
                        OnFailoverDetected(errorMessageAction);
                        return await PublishAsync(topic, message, errorMessageAction);
                    }
                    else
                    {
                        errorMessageAction?.Invoke($"PubSubServiceRedis->CustomPublish: {e.Message}, Trace: {e.StackTrace}");
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        public async Task<bool> SubscribeAsync(string topic, Func<string, string, Task> onMessage, Action<string>? errorMessageAction = null, CancellationToken cancellationToken = default)
        {
            if (topic != null && topic.Length > 0 && onMessage != null)
            {
                FailoverCheck();
                try
                {
                    RedisConnection.GetSubscriber().Subscribe(
                        topic,
                        (RedisChannel Channel, RedisValue Value) =>
                        {
                            onMessage?.Invoke(Channel, Value.ToString());
                        });
                }
                catch (Exception e)
                {
                    if (bFailoverMechanismEnabled && (e is RedisException || e is TimeoutException))
                    {
                        OnFailoverDetected(errorMessageAction);
                        return await SubscribeAsync(topic, onMessage, errorMessageAction);
                    }

                    errorMessageAction?.Invoke($"PubSubServiceRedis->CustomSubscribe: {e.Message}, Trace: {e.StackTrace}");
                    return false;
                }
                return true;
            }
            return false;
        }

        public async Task<bool> MarkUsedOnBucketEvent(string topic, CancellationToken cancellationToken = default)
        {
            return true;
        }

        public async Task<bool> UnmarkUsedOnBucketEvent(string topic, CancellationToken cancellationToken = default)
        {
            return true;
        }
        public async Task<List<string>?> GetTopicsUsedOnBucketEventAsync(Action<string>? errorMessageAction = null, CancellationToken cancellationToken = default)
        {
            return new List<string>();
        }
    }
}
