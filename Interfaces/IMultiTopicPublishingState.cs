namespace Opc.Ua.Cloud.Publisher.Interfaces
{
    using Opc.Ua.Cloud.Publisher.Models;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    public interface IMultiTopicPublishingState
    {
        Task<RegisterTopicPublishingResult> RegisterAndStartPublishingAsync(string topic, string publishedNodesJson, IPublishedNodesFileHandler publishedNodesFileHandler, string registrationKey = null);

        Task<UnregisterTopicPublishingResult> UnregisterAsync(string registrationKey);

        IReadOnlyCollection<string> ResolveTopics(string endpointUrl, string expandedNodeId);

        Task EnsureRestoredAsync(IPublishedNodesFileHandler publishedNodesFileHandler);

        IReadOnlyCollection<TopicPublishingRegistration> GetRegistrations();

        IReadOnlyCollection<ActiveTopicPublisher> GetActiveTopicPublishers();
    }
}
