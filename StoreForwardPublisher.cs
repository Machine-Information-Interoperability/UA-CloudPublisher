namespace Opc.Ua.Cloud.Publisher
{
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Opc.Ua.Cloud.Publisher.Interfaces;
    using System;
    using System.Collections.Concurrent;
    using System.Diagnostics;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;

    public class StoreForwardPublisher : IMessagePublisher
    {
        private class StoredMessageEnvelope
        {
            public string Topic { get; set; }

            public string PayloadBase64 { get; set; }
        }

        private IBrokerClient _client;
        private IBrokerClient _altClient;

        private readonly ILogger _logger;
        private readonly string _pathToStore;

        private readonly ConcurrentQueue<long> _lastMessageLatencies = new();
        private int _latencyCount;
        private long _latencySum;
        private int _forwardInProgress;

        public StoreForwardPublisher(ILoggerFactory loggerFactory, Settings.BrokerResolver brokerResolver)
        {
            _logger = loggerFactory.CreateLogger("StoreForwardPublisher");

            _pathToStore = Path.Combine(Directory.GetCurrentDirectory(), "store");
            if (!Directory.Exists(_pathToStore))
            {
                Directory.CreateDirectory(_pathToStore);
            }

            Diagnostics.Singleton.Info.StoredMessagesCount = Directory.GetFiles(_pathToStore).Length;

            if (Settings.Instance.UseKafka)
            {
                _client = brokerResolver("Kafka");
            }
            else
            {
                _client = brokerResolver("MQTT");
            }
        }

        public void ApplyNewClient(IBrokerClient client)
        {
            _client = client;
        }

        public void ApplyAltClient(IBrokerClient altClient)
        {
            _altClient = altClient;
        }

        public async Task<bool> SendMetadataAsync(byte[] message, string topic = null)
        {
            bool success = false;
            long startTime = Stopwatch.GetTimestamp();

            IBrokerClient client = _altClient ?? _client;

            try
            {
                if (client != null)
                {
                    await client.PublishMetadataAsync(message, topic).ConfigureAwait(false);
                    success = true;

                    Diagnostics.Singleton.Info.SentBytes += message.Length;
                    Diagnostics.Singleton.Info.SentMessages++;
                    Diagnostics.Singleton.Info.SentLastTime = DateTime.UtcNow;
                }
                else
                {
                    _logger.LogError("Broker client not available for sending metadata.");
                }
            }
            catch (Exception ex)
            {
                if (ex is AggregateException agg)
                {
                    ex = agg.Flatten();
                }

                _logger.LogError(ex, "Error while sending metadata message.");
            }

            RecordLatency((long)Stopwatch.GetElapsedTime(startTime).TotalMilliseconds);

            return success;
        }

        public async Task<bool> SendMessageAsync(byte[] message, string topic = null)
        {
            bool success = false;
            long startTime = Stopwatch.GetTimestamp();

            try
            {
                if (_client != null)
                {
                    await _client.PublishAsync(message, topic).ConfigureAwait(false);
                    success = true;

                    Diagnostics.Singleton.Info.SentBytes += message.Length;
                    Diagnostics.Singleton.Info.SentMessages++;
                    Diagnostics.Singleton.Info.SentLastTime = DateTime.UtcNow;

                    // forward stored messages in the background
                    _ = Task.Run(async () => await ForwardStoredMessageAsync().ConfigureAwait(false));
                }
                else
                {
                    throw new InvalidOperationException("Broker client not available for sending message.");
                }
            }
            catch (Exception ex)
            {
                if (ex is AggregateException agg)
                {
                    ex = agg.Flatten();
                }

                _logger.LogError(ex, "Error while sending message. Storing locally for later forward...");
                Diagnostics.Singleton.Info.FailedMessages++;
                
                StoredMessageEnvelope envelope = new StoredMessageEnvelope
                {
                    Topic = topic,
                    PayloadBase64 = Convert.ToBase64String(message)
                };

                byte[] bytes = System.Text.Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(envelope));
                await File.WriteAllBytesAsync(Path.Combine(_pathToStore, Path.GetRandomFileName()), bytes).ConfigureAwait(false);

            }

            RecordLatency((long)Stopwatch.GetElapsedTime(startTime).TotalMilliseconds);

            return success;
        }

        private async Task ForwardStoredMessageAsync()
        {
            // ensure only one forward task runs at a time to avoid duplicate sends/races on the same file
            if (Interlocked.Exchange(ref _forwardInProgress, 1) == 1)
            {
                return;
            }

            try
            {
                string[] filePaths = Directory.GetFiles(_pathToStore);
                if (filePaths.Length == 0)
                {
                    // nothing to send
                    return;
                }

                byte[] bytes = await File.ReadAllBytesAsync(filePaths[0]).ConfigureAwait(false);

                string topic = null;
                byte[] payload = bytes;

                try
                {
                    StoredMessageEnvelope envelope = JsonConvert.DeserializeObject<StoredMessageEnvelope>(System.Text.Encoding.UTF8.GetString(bytes));
                    if (envelope != null && !string.IsNullOrWhiteSpace(envelope.PayloadBase64))
                    {
                        payload = Convert.FromBase64String(envelope.PayloadBase64);
                        topic = envelope.Topic;
                    }
                }
                catch
                {
                    // Backward compatibility for old raw-byte store files.
                }

                if (string.IsNullOrWhiteSpace(topic) && string.IsNullOrWhiteSpace(Settings.Instance.BrokerMessageTopic))
                {
                    // Avoid blocking the replay queue forever with a message that can never be routed.
                    _logger.LogError("Dropping stored message '{FileName}' because neither stored topic nor Settings.BrokerMessageTopic is set.", Path.GetFileName(filePaths[0]));
                    File.Delete(filePaths[0]);
                    return;
                }

                await _client.PublishAsync(payload, topic).ConfigureAwait(false);

                File.Delete(filePaths[0]);

                Diagnostics.Singleton.Info.SentBytes += payload.Length;
                Diagnostics.Singleton.Info.SentMessages++;
                Diagnostics.Singleton.Info.FailedMessages = Math.Max(0, Diagnostics.Singleton.Info.FailedMessages - 1);
                Diagnostics.Singleton.Info.SentLastTime = DateTime.UtcNow;
                Diagnostics.Singleton.Info.StoredMessagesCount = Math.Max(0, filePaths.Length - 1);

                _logger.LogInformation("There are {Count} stored messages left to send.", filePaths.Length - 1);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Sending stored message failed, will retry later.");
            }
            finally
            {
                Interlocked.Exchange(ref _forwardInProgress, 0);
            }
        }

        private void RecordLatency(long milliseconds)
        {
            _lastMessageLatencies.Enqueue(milliseconds);
            Interlocked.Add(ref _latencySum, milliseconds);
            int count = Interlocked.Increment(ref _latencyCount);

            while (count > 100 && _lastMessageLatencies.TryDequeue(out long old))
            {
                Interlocked.Add(ref _latencySum, -old);
                count = Interlocked.Decrement(ref _latencyCount);
            }

            long sum = Interlocked.Read(ref _latencySum);
            Diagnostics.Singleton.Info.AverageMessageLatency = count > 0 ? sum / count : 0;
        }
    }
}
