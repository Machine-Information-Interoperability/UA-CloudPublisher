namespace Opc.Ua.Cloud.Publisher.Models
{
    using System;
    using System.Collections.Generic;

    public class TopicPublishingRegistration
    {
        public string RegistrationKey { get; set; }

        public string Topic { get; set; }

        public string PublishedNodesJson { get; set; }

        public List<string> RoutingKeys { get; set; } = new List<string>();

        public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
    }

    public class RegisterTopicPublishingResult
    {
        public bool IsDuplicate { get; set; }

        public bool IsTopicAlreadyActive { get; set; }

        public string RegistrationKey { get; set; }

        public string Topic { get; set; }

        public int ActiveRegistrationCountForTopic { get; set; }

        public int ActiveRegistrationCount { get; set; }
    }

    public class ActiveTopicPublisher
    {
        public string Topic { get; set; }

        public string PublishedNodesJson { get; set; }
    }

    public class UnregisterTopicPublishingResult
    {
        public bool Found { get; set; }

        public string RegistrationKey { get; set; }

        public string Topic { get; set; }

        public int ActiveRegistrationCountForTopic { get; set; }

        public bool IsTopicStillActive => ActiveRegistrationCountForTopic > 0;

        public string PublishedNodesJson { get; set; }

        public int ActiveRegistrationCount { get; set; }
    }
}
