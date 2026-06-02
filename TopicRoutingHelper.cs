namespace Opc.Ua.Cloud.Publisher
{
    using System;

    internal static class TopicRoutingHelper
    {
        public static string ResolveMetadataTopic(string topic, string transportName)
        {
            if (!string.IsNullOrWhiteSpace(topic))
            {
                return topic;
            }

            if (!string.IsNullOrWhiteSpace(Settings.Instance.BrokerMetadataTopic))
            {
                return Settings.Instance.BrokerMetadataTopic;
            }

            if (!string.IsNullOrWhiteSpace(Settings.Instance.BrokerMessageTopic))
            {
                return Settings.Instance.BrokerMessageTopic;
            }

            throw new InvalidOperationException($"Cannot publish {transportName} metadata because no metadata topic was provided and both Settings.BrokerMetadataTopic and Settings.BrokerMessageTopic are empty.");
        }
    }
}
