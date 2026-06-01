namespace Opc.Ua.Cloud.Publisher.Controllers
{
    using Microsoft.AspNetCore.Mvc;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Opc.Ua;
    using Opc.Ua.Cloud.Publisher.Interfaces;
    using Opc.Ua.Cloud.Publisher.Models;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    /// <summary>
    /// REST API controller for multi-topic OPC UA node publishing to MQTT.
    /// </summary>
    [ApiController]
    [Route("api/publishing")]
    public class PublishingApiController : ControllerBase
    {
        private readonly ILogger _logger;
        private readonly IPublishedNodesFileHandler _publishedNodesFileHandler;
        private readonly IMultiTopicPublishingState _multiTopicState;
        private readonly IUAClient _uaClient;


        /// <summary>
        /// Initializes a new instance of the <see cref="PublishingApiController"/> class.
        /// </summary>
        /// <param name="loggerFactory">The logger factory for creating loggers.</param>
        /// <param name="publishedNodesFileHandler">The handler for published nodes file operations.</param>
        /// <param name="multiTopicState">The multi-topic publishing state manager.</param>
        /// <param name="uaClient">The OPC UA client used for unpublishing nodes when stopping a registration.</param>
        public PublishingApiController(ILoggerFactory loggerFactory, IPublishedNodesFileHandler publishedNodesFileHandler, IMultiTopicPublishingState multiTopicState, IUAClient uaClient)
        {
            _logger = loggerFactory.CreateLogger("PublishingApiController");
            _publishedNodesFileHandler = publishedNodesFileHandler;
            _multiTopicState = multiTopicState;
            _uaClient = uaClient;
        }

        /// <summary>
        /// Register and start publishing OPC UA nodes to a specified MQTT topic.
        /// </summary>
        /// <remarks>
        /// This endpoint requires the EnableMultiTopicPublishing setting to be enabled.
        /// 
        /// If the same (topic + publishednodes) combination is already active,
        /// it returns a 200 OK response indicating the combo is already active (no duplicate publisher created).
        /// 
        /// If a new combination is provided, it registers the combo, persists it locally, and starts publishing to the MQTT broker.
        /// 
        /// Example request:
        /// POST /api/publishing/publishednodes?topic=factory/line1/telemetry
        /// Content-Type: application/json
        /// 
        /// [
        ///   {
        ///     "EndpointUrl": "opc.tcp://server:4840",
        ///     "OpcNodes": [
        ///       {
        ///         "Id": "ns=2;i=1001",
        ///         "OpcSamplingInterval": 1000,
        ///         "OpcPublishingInterval": 1000
        ///       }
        ///     ]
        ///   }
        /// ]
        /// </remarks>
        /// <param name="topic">MQTT topic name where telemetry will be published (query parameter).</param>
        /// <param name="registrationKey">Optional registration ID to use as the registration key. If omitted, a deterministic key is generated from the topic and node configuration.</param>
        /// <param name="publishedNodes">Array of published node configurations in publishednodes.json format (request body).</param>
        /// <returns>
        /// 202 Accepted if new registration started successfully, or 200 OK if duplicate combo already active.
        /// 503 Service Unavailable if multi-topic publishing is not enabled.
        /// 400 Bad Request if topic is missing or publishedNodes array is invalid.
        /// 409 Conflict if Kafka mode is enabled.
        /// 500 Internal Server Error if publishing failed.
        /// </returns>
        [HttpPost("publishednodes")]
        [Consumes("application/json")]
        public async Task<IActionResult> StartPublishingAsync([FromQuery(Name = "topic")] string topic, [FromQuery(Name = "registrationKey")] string registrationKey, [FromBody] List<PublishNodesInterfaceModel> publishedNodes)
        {
            if (!Settings.Instance.EnableMultiTopicPublishing)
            {
                return StatusCode(503, new { error = "Multi-topic publishing is not enabled. Set 'EnableMultiTopicPublishing' to true in settings." });
            }

            if (string.IsNullOrWhiteSpace(topic))
            {
                return BadRequest(new { error = "Query parameter 'topic' is required." });
            }

            if (publishedNodes == null || publishedNodes.Count == 0)
            {
                return BadRequest(new { error = "Request body must be a JSON array of published nodes with at least one entry." });
            }

            if (Settings.Instance.UseKafka)
            {
                return Conflict(new { error = "This endpoint requires MQTT. Kafka mode is currently enabled." });
            }

            try
            {
                string publishedNodesJson = JsonConvert.SerializeObject(publishedNodes, Formatting.None);

                RegisterTopicPublishingResult result = await _multiTopicState
                    .RegisterAndStartPublishingAsync(topic, publishedNodesJson, _publishedNodesFileHandler, registrationKey)
                    .ConfigureAwait(false);

                if (result.IsDuplicate)
                {
                    return Ok(new
                    {
                        status = "Already active.",
                        topic = result.Topic,
                        registrationKey = result.RegistrationKey,
                        activeRegistrationCountForTopic = result.ActiveRegistrationCountForTopic,
                        activeRegistrationCount = result.ActiveRegistrationCount
                    });
                }

                _logger.LogInformation(
                    result.IsTopicAlreadyActive
                        ? "Added registration for already active MQTT topic '{Topic}'."
                        : "Started publishing from API request to MQTT topic '{Topic}'.",
                    topic);

                return Accepted(new
                {
                    status = result.IsTopicAlreadyActive
                        ? "Registration added. Topic publisher was already active."
                        : "Publishing started.",
                    topic = result.Topic,
                    registrationKey = result.RegistrationKey,
                    isTopicAlreadyActive = result.IsTopicAlreadyActive,
                    activeRegistrationCountForTopic = result.ActiveRegistrationCountForTopic,
                    activeRegistrationCount = result.ActiveRegistrationCount,
                    progress = _publishedNodesFileHandler.Progress
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to start publishing from API request.");
                return StatusCode(500, new { error = ex.Message });
            }
        }

        /// <summary>
        /// Remove one registration key and stop publishing for the topic when the last key is removed.
        /// </summary>
        /// <param name="registrationKey">Registration key returned by the start publishing endpoint.</param>
        /// <returns>
        /// 200 OK when registration is removed successfully.
        /// 404 Not Found when registration key does not exist.
        /// 400 Bad Request when registration key is missing.
        /// 503 Service Unavailable if multi-topic publishing is not enabled.
        /// </returns>
        [HttpDelete("publishednodes/{registrationKey}")]
        public async Task<IActionResult> StopPublishingAsync([FromRoute] string registrationKey)
        {
            if (!Settings.Instance.EnableMultiTopicPublishing)
            {
                return StatusCode(503, new { error = "Multi-topic publishing is not enabled. Set 'EnableMultiTopicPublishing' to true in settings." });
            }

            if (string.IsNullOrWhiteSpace(registrationKey))
            {
                return BadRequest(new { error = "Route parameter 'registrationKey' is required." });
            }

            try
            {
                UnregisterTopicPublishingResult result = await _multiTopicState.UnregisterAsync(registrationKey).ConfigureAwait(false);
                if (!result.Found)
                {
                    return NotFound(new
                    {
                        error = "Registration was not found.",
                        registrationKey,
                        activeRegistrationCount = result.ActiveRegistrationCount
                    });
                }

                int unpublishedNodeCount = 0;
                int skippedNodeCount = 0;
                int failedNodeCount = 0;

                bool topicStopped = !result.IsTopicStillActive;
                if (topicStopped && !string.IsNullOrWhiteSpace(result.PublishedNodesJson))
                {
                    HashSet<string> stillActiveNodeKeys = BuildActiveNodeKeySet();
                    HashSet<string> processedNodeKeys = new HashSet<string>(StringComparer.Ordinal);
                    foreach (NodePublishingModel node in ExtractNodesForUnpublish(result.PublishedNodesJson))
                    {
                        string nodeKey = BuildNodeKey(node.EndpointUrl, node.ExpandedNodeId?.ToString());
                        if (!processedNodeKeys.Add(nodeKey))
                        {
                            continue;
                        }

                        if (stillActiveNodeKeys.Contains(nodeKey))
                        {
                            skippedNodeCount++;
                            continue;
                        }

                        try
                        {
                            await _uaClient.UnpublishNodeAsync(node).ConfigureAwait(false);
                            unpublishedNodeCount++;
                        }
                        catch (Exception ex)
                        {
                            failedNodeCount++;
                            _logger.LogWarning(ex, "Failed to unpublish node '{NodeId}' from endpoint '{EndpointUrl}' during stop publishing.", node.ExpandedNodeId, node.EndpointUrl);
                        }
                    }
                }

                _logger.LogInformation(
                    "Removed registration '{RegistrationKey}' for topic '{Topic}'. Topic stopped: {TopicStopped}.",
                    result.RegistrationKey,
                    result.Topic,
                    topicStopped);

                return Ok(new
                {
                    status = topicStopped ? "Registration removed. Topic publishing stopped." : "Registration removed. Topic is still active.",
                    registrationKey = result.RegistrationKey,
                    topic = result.Topic,
                    topicStopped,
                    activeRegistrationCountForTopic = result.ActiveRegistrationCountForTopic,
                    unpublishedNodeCount,
                    skippedNodeCount,
                    failedNodeCount,
                    activeRegistrationCount = result.ActiveRegistrationCount
                });
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failed to stop publishing for registration '{RegistrationKey}'.", registrationKey);
                return StatusCode(500, new { error = ex.Message });
            }
        }

        private HashSet<string> BuildActiveNodeKeySet()
        {
            HashSet<string> keys = new HashSet<string>(StringComparer.Ordinal);
            foreach (ActiveTopicPublisher activeTopicPublisher in _multiTopicState.GetActiveTopicPublishers())
            {
                foreach (string nodeKey in ExtractNodeKeys(activeTopicPublisher.PublishedNodesJson))
                {
                    keys.Add(nodeKey);
                }
            }

            return keys;
        }

        private static IEnumerable<string> ExtractNodeKeys(string publishedNodesJson)
        {
            foreach (NodePublishingModel node in ExtractNodesForUnpublish(publishedNodesJson))
            {
                yield return BuildNodeKey(node.EndpointUrl, node.ExpandedNodeId?.ToString());
            }
        }

        private static IEnumerable<NodePublishingModel> ExtractNodesForUnpublish(string publishedNodesJson)
        {
            List<PublishNodesInterfaceModel> entries = JsonConvert.DeserializeObject<List<PublishNodesInterfaceModel>>(publishedNodesJson) ?? new List<PublishNodesInterfaceModel>();
            foreach (PublishNodesInterfaceModel entry in entries)
            {
                string normalizedEndpoint = NormalizeEndpoint(entry.EndpointUrl);

                if (entry.OpcEvents != null)
                {
                    foreach (EventModel opcEvent in entry.OpcEvents.Where(v => !string.IsNullOrWhiteSpace(v.ExpandedNodeId)))
                    {
                        yield return new NodePublishingModel
                        {
                            EndpointUrl = normalizedEndpoint,
                            ExpandedNodeId = ExpandedNodeId.Parse(opcEvent.ExpandedNodeId)
                        };
                    }
                }

                if (entry.OpcNodes != null)
                {
                    foreach (VariableModel opcNode in entry.OpcNodes.Where(v => !string.IsNullOrWhiteSpace(v.Id)))
                    {
                        yield return new NodePublishingModel
                        {
                            EndpointUrl = normalizedEndpoint,
                            ExpandedNodeId = ExpandedNodeId.Parse(opcNode.Id)
                        };
                    }
                }
            }
        }

        private static string BuildNodeKey(string endpointUrl, string expandedNodeId)
        {
            return $"{NormalizeEndpoint(endpointUrl)}|{expandedNodeId?.Trim()}";
        }

        private static string NormalizeEndpoint(string endpointUrl)
        {
            if (string.IsNullOrWhiteSpace(endpointUrl))
            {
                return string.Empty;
            }

            return Uri.TryCreate(endpointUrl, UriKind.Absolute, out Uri uri)
                ? uri.ToString()
                : endpointUrl.Trim();
        }
    }
}

