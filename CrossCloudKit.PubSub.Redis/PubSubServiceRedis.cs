// Copyright (c) 2022- Burak Kara, MIT License
// See LICENSE file in the project root for full license information.

using CrossCloudKit.Interfaces;
using CrossCloudKit.Memory.Redis.Common;
using StackExchange.Redis;
using CrossCloudKit.Utilities.Common;

namespace CrossCloudKit.PubSub.Redis
{
    public class PubSubServiceRedis : RedisCommonFunctionalities, IPubSubService
    {
        private bool _disposed;

        /// <summary>
        ///
        /// <para>PubSubServiceRedis: Parameterized Constructor</para>
        /// <para>Note: Redis Pub/Sub service does not keep messages in a permanent queue, therefore if there is not any listener, message will be lost, unlike other Pub/Sub services.</para>
        ///
        /// <para>Parameters:</para>
        /// <param name="connectionOptions">Redis connection configuration options.</param>
        ///
        /// </summary>
        public PubSubServiceRedis(RedisConnectionOptions connectionOptions) : base(connectionOptions) {}

        public bool IsInitialized => Initialized;

        /// <inheritdoc />
        public async Task<OperationResult<bool>> DeleteTopicAsync(string topic, CancellationToken cancellationToken = default)
        {
            if (_disposed)
                return OperationResult<bool>.Success(true); // Already disposed, consider topic deleted
            if (!IsInitialized)
                return OperationResult<bool>.Failure("Redis connection is not initialized");
            if (string.IsNullOrEmpty(topic))
                return OperationResult<bool>.Failure("Topic cannot be empty.");
            if (RedisConnection == null)
                return OperationResult<bool>.Failure("Redis connection is not initialized");

            var result = await ExecuteRedisOperationAsync(async _ =>
            {
                await RedisConnection.GetSubscriber().UnsubscribeAsync(RedisChannel.Literal(topic)).ConfigureAwait(false);
                return true;
            }, cancellationToken).ConfigureAwait(false);

            return result;
        }

        /// <inheritdoc />
        public Task<OperationResult<bool>> EnsureTopicExistsAsync(string topic, CancellationToken cancellationToken = default)
        {
            return Task.FromResult(!IsInitialized ? OperationResult<bool>.Failure("Redis connection is not initialized") : OperationResult<bool>.Success(true));
        }

        /// <inheritdoc />
        public async Task<OperationResult<bool>> PublishAsync(string topic, string message, CancellationToken cancellationToken = default)
        {
            if (_disposed)
                return OperationResult<bool>.Failure("Service has been disposed");
            if (!IsInitialized)
                return OperationResult<bool>.Failure("Redis connection is not initialized");
            if (string.IsNullOrEmpty(topic)
                || string.IsNullOrEmpty(message))
                return OperationResult<bool>.Failure("Topic and message cannot be empty.");
            if (RedisConnection == null)
                return OperationResult<bool>.Failure("Redis connection is not initialized");

            return await ExecuteRedisOperationAsync(async _ =>
            {
                await RedisConnection.GetDatabase().PublishAsync(RedisChannel.Literal(topic), message);
                return true;
            }, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<OperationResult<bool>> SubscribeAsync(string topic, Func<string, string, Task>? onMessage, Action<Exception>? onError = null, CancellationToken cancellationToken = default)
        {
            if (_disposed)
                return OperationResult<bool>.Failure("Service has been disposed");
            if (!IsInitialized)
                return OperationResult<bool>.Failure("Redis connection is not initialized");
            if (string.IsNullOrEmpty(topic))
                return OperationResult<bool>.Failure("Topic and message cannot be empty.");
            if (onMessage == null)
                return OperationResult<bool>.Failure("Callback cannot be null.");
            if (RedisConnection == null)
                return OperationResult<bool>.Failure("Redis connection is not initialized");

            return await ExecuteRedisOperationAsync(async _ =>
            {
                await RedisConnection.GetSubscriber().SubscribeAsync(
                    RedisChannel.Literal(topic),
                    (channel, value) =>
                    {
                        onMessage.Invoke(channel!, value.ToString());
                    });
                return true;
            }, cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<OperationResult<bool>> MarkUsedOnBucketEvent(string topic, CancellationToken cancellationToken = default)
        {
            if (_disposed)
                return OperationResult<bool>.Failure("Service has been disposed");
            if (string.IsNullOrEmpty(topic))
                return OperationResult<bool>.Failure("Topic cannot be empty.");
            if (!IsInitialized)
                return OperationResult<bool>.Failure("Redis connection is not initialized");

            var topics = await GetTopicsUsedOnBucketEventAsync(cancellationToken);
            if (!topics.IsSuccessful || topics.Data == null)
                return OperationResult<bool>.Failure(topics.ErrorMessage!);
            if (topics.Data.Contains(topic))
                return OperationResult<bool>.Success(true);

            return await Common_PushToListAsync(
                SystemClassMemoryScopeInstance,
                UsedOnBucketEventListName,
                [
                    new PrimitiveType(topic)
                ],
                true,
                false,
                false,
                this,
                cancellationToken).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task<OperationResult<bool>> UnmarkUsedOnBucketEvent(string topic, CancellationToken cancellationToken = default)
        {
            if (_disposed)
                return OperationResult<bool>.Failure("Service has been disposed");
            if (string.IsNullOrEmpty(topic))
                return OperationResult<bool>.Failure("Topic cannot be empty.");
            if (!IsInitialized)
                return OperationResult<bool>.Failure("Redis connection is not initialized");

            var result = await Common_RemoveElementsFromListAsync(
                SystemClassMemoryScopeInstance,
                UsedOnBucketEventListName,
                [
                    new PrimitiveType(topic)
                ],
                false,
                this,
                cancellationToken).ConfigureAwait(false);
            return result.IsSuccessful ? OperationResult<bool>.Success(true) : OperationResult<bool>.Failure(result.ErrorMessage ?? string.Empty);
        }

        /// <inheritdoc />
        public async Task<OperationResult<List<string>>> GetTopicsUsedOnBucketEventAsync(CancellationToken cancellationToken = default)
        {
            if (_disposed)
                return OperationResult<List<string>>.Failure("Service has been disposed");
            if (!IsInitialized)
                return OperationResult<List<string>>.Failure("Redis connection is not initialized");
            var result = await Common_GetAllElementsOfListAsync(
                SystemClassMemoryScopeInstance,
                UsedOnBucketEventListName,
                cancellationToken).ConfigureAwait(false);
            if (!result.IsSuccessful || result.Data == null) return OperationResult<List<string>>.Failure(result.ErrorMessage ?? string.Empty);
            return OperationResult<List<string>>.Success(result.Data.Select(x => x.ToString()).ToList());
        }

        /// <summary>
        /// Dispose the service asynchronously
        /// </summary>
        public override async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;

            _disposed = true;
            await base.DisposeAsync().ConfigureAwait(false);

            GC.SuppressFinalize(this);
        }

        private class SystemClassMemoryScope : IMemoryServiceScope
        {
            public string Compile()
            {
                return Scope;
            }
            private const string Scope = "CrossCloudKit.PubSub.Redis.PubSubServiceRedis.SystemMemoryScope";
        }
        private static readonly SystemClassMemoryScope SystemClassMemoryScopeInstance = new();
        private const string UsedOnBucketEventListName = "user_on_bucket_event_list";
    }
}
