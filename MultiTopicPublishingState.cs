namespace Opc.Ua.Cloud.Publisher
{
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using Opc.Ua.Cloud.Publisher.Interfaces;
    using Opc.Ua.Cloud.Publisher.Models;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Security.Cryptography;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    public class MultiTopicPublishingState : IMultiTopicPublishingState
    {
        private readonly ILogger _logger;
        private readonly SemaphoreSlim _stateLock = new(1, 1);

        private readonly Dictionary<string, TopicPublishingRegistration> _registrationsByKey = new();
        private readonly Dictionary<string, ActiveTopicPublisher> _activeTopicPublishersByTopic = new(StringComparer.Ordinal);
        private readonly Dictionary<string, HashSet<string>> _topicsByRoutingKey = new();

        private readonly string _stateFilePath = Path.Combine(Directory.GetCurrentDirectory(), "settings", "topic-publishing-state.json");

        public MultiTopicPublishingState(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger("MultiTopicPublishingState");
            LoadFromDisk();
        }

        public IReadOnlyCollection<TopicPublishingRegistration> GetRegistrations()
        {
            return _registrationsByKey.Values.ToList();
        }

        public IReadOnlyCollection<ActiveTopicPublisher> GetActiveTopicPublishers()
        {
            return _activeTopicPublishersByTopic.Values
                .Select(v => new ActiveTopicPublisher
                {
                    Topic = v.Topic,
                    PublishedNodesJson = v.PublishedNodesJson
                })
                .ToList();
        }

        public IReadOnlyCollection<string> ResolveTopics(string endpointUrl, string expandedNodeId)
        {
            if (string.IsNullOrWhiteSpace(expandedNodeId))
            {
                return Array.Empty<string>();
            }

            string normalizedEndpoint = NormalizeEndpoint(endpointUrl);
            HashSet<string> resolvedTopics = new HashSet<string>(StringComparer.Ordinal);

            string key = BuildRoutingKey(normalizedEndpoint, expandedNodeId);
            if (_topicsByRoutingKey.TryGetValue(key, out HashSet<string> topics))
            {
                resolvedTopics.UnionWith(topics);
            }

            string fallbackKey = BuildRoutingKey("*", expandedNodeId);
            if (_topicsByRoutingKey.TryGetValue(fallbackKey, out HashSet<string> fallbackTopics))
            {
                resolvedTopics.UnionWith(fallbackTopics);
            }

            string relaxedNodeId = NormalizeNodeIdRelaxed(expandedNodeId);
            if (!string.Equals(relaxedNodeId, expandedNodeId, StringComparison.Ordinal))
            {
                string relaxedKey = BuildRoutingKey(normalizedEndpoint, relaxedNodeId);
                if (_topicsByRoutingKey.TryGetValue(relaxedKey, out HashSet<string> relaxedTopics))
                {
                    resolvedTopics.UnionWith(relaxedTopics);
                }

                string relaxedFallbackKey = BuildRoutingKey("*", relaxedNodeId);
                if (_topicsByRoutingKey.TryGetValue(relaxedFallbackKey, out HashSet<string> relaxedFallbackTopics))
                {
                    resolvedTopics.UnionWith(relaxedFallbackTopics);
                }
            }

            return resolvedTopics.Count > 0 ? resolvedTopics.ToArray() : Array.Empty<string>();
        }

        public async Task EnsureRestoredAsync(IPublishedNodesFileHandler publishedNodesFileHandler)
        {
            if (Settings.Instance.AutoLoadPersistedNodes)
            {
                // Existing persistency restore path already republishes nodes.
                return;
            }

            ActiveTopicPublisher[] activeTopicPublishers = _activeTopicPublishersByTopic.Values.ToArray();
            if (activeTopicPublishers.Length == 0)
            {
                return;
            }

            foreach (ActiveTopicPublisher activeTopicPublisher in activeTopicPublishers)
            {
                await publishedNodesFileHandler.ParseFileAsync(Encoding.UTF8.GetBytes(activeTopicPublisher.PublishedNodesJson)).ConfigureAwait(false);
            }

            _logger.LogInformation("Restored {Count} active topic publisher(s) from state file.", activeTopicPublishers.Length);
        }

        public async Task<RegisterTopicPublishingResult> RegisterAndStartPublishingAsync(string topic, string publishedNodesJson, IPublishedNodesFileHandler publishedNodesFileHandler, string registrationKey = null)
        {
            if (string.IsNullOrWhiteSpace(topic))
            {
                throw new ArgumentException("Topic must be provided.", nameof(topic));
            }

            if (string.IsNullOrWhiteSpace(publishedNodesJson))
            {
                throw new ArgumentException("publishednodes.json content must be provided.", nameof(publishedNodesJson));
            }

            JToken json = JToken.Parse(publishedNodesJson);
            if (json.Type != JTokenType.Array)
            {
                throw new ArgumentException("publishednodes.json body must be a JSON array.", nameof(publishedNodesJson));
            }

            string normalizedJson = json.ToString(Formatting.None);
            registrationKey = !string.IsNullOrWhiteSpace(registrationKey)
                ? registrationKey.Trim()
                : ComputeRegistrationKey(topic, normalizedJson);
            List<string> routingKeys = ExtractRoutingKeys((JArray)json);

            await _stateLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (_registrationsByKey.ContainsKey(registrationKey))
                {
                    int duplicateTopicCount = _registrationsByKey.Values.Count(v => string.Equals(v.Topic, topic, StringComparison.Ordinal));
                    return new RegisterTopicPublishingResult
                    {
                        IsDuplicate = true,
                        IsTopicAlreadyActive = _activeTopicPublishersByTopic.ContainsKey(topic),
                        RegistrationKey = registrationKey,
                        Topic = topic,
                        ActiveRegistrationCountForTopic = duplicateTopicCount,
                        ActiveRegistrationCount = _registrationsByKey.Count
                    };
                }

                bool isTopicAlreadyActive = _activeTopicPublishersByTopic.ContainsKey(topic);

                TopicPublishingRegistration registration = new TopicPublishingRegistration
                {
                    RegistrationKey = registrationKey,
                    Topic = topic,
                    PublishedNodesJson = normalizedJson,
                    RoutingKeys = routingKeys,
                    CreatedAtUtc = DateTime.UtcNow
                };

                _registrationsByKey.Add(registrationKey, registration);

                if (!isTopicAlreadyActive)
                {
                    _activeTopicPublishersByTopic[topic] = new ActiveTopicPublisher
                    {
                        Topic = topic,
                        PublishedNodesJson = normalizedJson
                    };

                    AddRouting(registration.Topic, registration.RoutingKeys);
                }

                SaveToDisk();

                try
                {
                    if (!isTopicAlreadyActive)
                    {
                        await publishedNodesFileHandler.ParseFileAsync(Encoding.UTF8.GetBytes(normalizedJson)).ConfigureAwait(false);
                    }
                }
                catch
                {
                    // Roll back registration if publish initialization fails.
                    _registrationsByKey.Remove(registrationKey);

                    if (!isTopicAlreadyActive)
                    {
                        _activeTopicPublishersByTopic.Remove(topic);
                    }

                    RebuildRouting();
                    SaveToDisk();
                    throw;
                }

                int activeRegistrationCountForTopic = _registrationsByKey.Values.Count(v => string.Equals(v.Topic, topic, StringComparison.Ordinal));

                return new RegisterTopicPublishingResult
                {
                    IsDuplicate = false,
                    IsTopicAlreadyActive = isTopicAlreadyActive,
                    RegistrationKey = registrationKey,
                    Topic = topic,
                    ActiveRegistrationCountForTopic = activeRegistrationCountForTopic,
                    ActiveRegistrationCount = _registrationsByKey.Count
                };
            }
            finally
            {
                _stateLock.Release();
            }
        }

        public async Task<UnregisterTopicPublishingResult> UnregisterAsync(string registrationKey)
        {
            if (string.IsNullOrWhiteSpace(registrationKey))
            {
                throw new ArgumentException("Registration key must be provided.", nameof(registrationKey));
            }

            await _stateLock.WaitAsync().ConfigureAwait(false);
            try
            {
                if (!_registrationsByKey.TryGetValue(registrationKey, out TopicPublishingRegistration registration))
                {
                    return new UnregisterTopicPublishingResult
                    {
                        Found = false,
                        RegistrationKey = registrationKey,
                        ActiveRegistrationCountForTopic = 0,
                        ActiveRegistrationCount = _registrationsByKey.Count
                    };
                }

                _registrationsByKey.Remove(registrationKey);
                int activeRegistrationCountForTopic = _registrationsByKey.Values.Count(r =>
                    string.Equals(r.Topic, registration.Topic, StringComparison.Ordinal));

                string activeTopicPublishedNodesJson = null;
                if (activeRegistrationCountForTopic == 0)
                {
                    if (_activeTopicPublishersByTopic.TryGetValue(registration.Topic, out ActiveTopicPublisher activeTopicPublisher))
                    {
                        activeTopicPublishedNodesJson = activeTopicPublisher.PublishedNodesJson;
                    }

                    _activeTopicPublishersByTopic.Remove(registration.Topic);
                }

                RebuildRouting();
                SaveToDisk();

                return new UnregisterTopicPublishingResult
                {
                    Found = true,
                    RegistrationKey = registration.RegistrationKey,
                    Topic = registration.Topic,
                    ActiveRegistrationCountForTopic = activeRegistrationCountForTopic,
                    PublishedNodesJson = activeTopicPublishedNodesJson,
                    ActiveRegistrationCount = _registrationsByKey.Count
                };
            }
            finally
            {
                _stateLock.Release();
            }
        }

        private void AddRouting(string topic, IReadOnlyCollection<string> routingKeys)
        {
            foreach (string routingKey in routingKeys)
            {
                if (!_topicsByRoutingKey.TryGetValue(routingKey, out HashSet<string> topics))
                {
                    topics = new HashSet<string>(StringComparer.Ordinal);
                    _topicsByRoutingKey[routingKey] = topics;
                }

                topics.Add(topic);
            }
        }

        private void RebuildRouting()
        {
            _topicsByRoutingKey.Clear();

            foreach (ActiveTopicPublisher activeTopicPublisher in _activeTopicPublishersByTopic.Values)
            {
                if (string.IsNullOrWhiteSpace(activeTopicPublisher.PublishedNodesJson))
                {
                    continue;
                }

                try
                {
                    JToken json = JToken.Parse(activeTopicPublisher.PublishedNodesJson);
                    if (json is JArray jsonArray)
                    {
                        AddRouting(activeTopicPublisher.Topic, ExtractRoutingKeys(jsonArray));
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to rebuild routing for topic '{Topic}'.", activeTopicPublisher.Topic);
                }
            }
        }

        private IEnumerable<TopicPublishingRegistration> GetTopicPublisherRegistrations()
        {
            return _registrationsByKey.Values
                .GroupBy(v => v.Topic, StringComparer.Ordinal)
                .Select(group => group
                    .OrderBy(v => v.CreatedAtUtc)
                    .ThenBy(v => v.RegistrationKey, StringComparer.Ordinal)
                    .First());
        }

        private List<string> ExtractRoutingKeys(JArray json)
        {
            List<PublishNodesInterfaceModel> entries = json.ToObject<List<PublishNodesInterfaceModel>>();
            List<string> keys = new List<string>();

            if (entries == null)
            {
                return keys;
            }

            foreach (PublishNodesInterfaceModel entry in entries)
            {
                string endpoint = NormalizeEndpoint(entry.EndpointUrl);

                if (entry.OpcNodes != null)
                {
                    foreach (VariableModel node in entry.OpcNodes)
                    {
                        if (!string.IsNullOrWhiteSpace(node.Id))
                        {
                            keys.Add(BuildRoutingKey(endpoint, node.Id));
                            keys.Add(BuildRoutingKey("*", node.Id));

                            string relaxedNodeId = NormalizeNodeIdRelaxed(node.Id);
                            if (!string.Equals(relaxedNodeId, node.Id, StringComparison.Ordinal))
                            {
                                keys.Add(BuildRoutingKey(endpoint, relaxedNodeId));
                                keys.Add(BuildRoutingKey("*", relaxedNodeId));
                            }
                        }
                    }
                }

                if (entry.OpcEvents != null)
                {
                    foreach (EventModel opcEvent in entry.OpcEvents)
                    {
                        if (!string.IsNullOrWhiteSpace(opcEvent.ExpandedNodeId))
                        {
                            keys.Add(BuildRoutingKey(endpoint, opcEvent.ExpandedNodeId));
                            keys.Add(BuildRoutingKey("*", opcEvent.ExpandedNodeId));

                            string relaxedNodeId = NormalizeNodeIdRelaxed(opcEvent.ExpandedNodeId);
                            if (!string.Equals(relaxedNodeId, opcEvent.ExpandedNodeId, StringComparison.Ordinal))
                            {
                                keys.Add(BuildRoutingKey(endpoint, relaxedNodeId));
                                keys.Add(BuildRoutingKey("*", relaxedNodeId));
                            }
                        }
                    }
                }
            }

            return keys.Distinct(StringComparer.Ordinal).OrderBy(v => v, StringComparer.Ordinal).ToList();
        }

        private static string ComputeRegistrationKey(string topic, string normalizedJson)
        {
            byte[] hash = SHA256.HashData(Encoding.UTF8.GetBytes($"{topic}|{normalizedJson}"));
            return Convert.ToHexString(hash);
        }

        private static string BuildRoutingKey(string endpointUrl, string expandedNodeId)
        {
            return $"{endpointUrl}|{expandedNodeId}";
        }

        private static string NormalizeEndpoint(string endpointUrl)
        {
            if (string.IsNullOrWhiteSpace(endpointUrl))
            {
                return string.Empty;
            }

            if (Uri.TryCreate(endpointUrl, UriKind.Absolute, out Uri uri))
            {
                return uri.ToString();
            }

            return endpointUrl.Trim();
        }

        private static string NormalizeNodeIdRelaxed(string nodeId)
        {
            if (string.IsNullOrWhiteSpace(nodeId))
            {
                return string.Empty;
            }

            string trimmed = nodeId.Trim();

            // Normalize nsu=...;i=123 and ns=2;i=123 to a shared fallback key i=123.
            int namespaceSeparatorIndex = trimmed.IndexOf(';');
            if (namespaceSeparatorIndex > 0)
            {
                string namespacePrefix = trimmed.Substring(0, namespaceSeparatorIndex);
                if (namespacePrefix.StartsWith("nsu=", StringComparison.OrdinalIgnoreCase) ||
                    namespacePrefix.StartsWith("ns=", StringComparison.OrdinalIgnoreCase))
                {
                    return trimmed.Substring(namespaceSeparatorIndex + 1);
                }
            }

            return trimmed;
        }

        private void LoadFromDisk()
        {
            try
            {
                if (!File.Exists(_stateFilePath))
                {
                    return;
                }

                string content = File.ReadAllText(_stateFilePath, Encoding.UTF8);
                List<TopicPublishingRegistration> registrations = JsonConvert.DeserializeObject<List<TopicPublishingRegistration>>(content);
                if (registrations == null)
                {
                    return;
                }

                _registrationsByKey.Clear();
                foreach (TopicPublishingRegistration registration in registrations)
                {
                    if (string.IsNullOrWhiteSpace(registration.RegistrationKey) || string.IsNullOrWhiteSpace(registration.Topic))
                    {
                        continue;
                    }

                    registration.RoutingKeys ??= new List<string>();
                    _registrationsByKey[registration.RegistrationKey] = registration;
                }

                _activeTopicPublishersByTopic.Clear();
                foreach (TopicPublishingRegistration registration in GetTopicPublisherRegistrations())
                {
                    _activeTopicPublishersByTopic[registration.Topic] = new ActiveTopicPublisher
                    {
                        Topic = registration.Topic,
                        PublishedNodesJson = registration.PublishedNodesJson
                    };
                }

                RebuildRouting();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to load topic publishing state file.");
            }
        }

        private void SaveToDisk()
        {
            try
            {
                Directory.CreateDirectory(Path.GetDirectoryName(_stateFilePath));
                string payload = JsonConvert.SerializeObject(_registrationsByKey.Values.OrderBy(r => r.CreatedAtUtc), Formatting.Indented);
                File.WriteAllText(_stateFilePath, payload, Encoding.UTF8);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to persist topic publishing state file.");
            }
        }
    }
}

