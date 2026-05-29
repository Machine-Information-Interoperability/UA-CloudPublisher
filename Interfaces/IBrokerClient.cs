using System.Threading.Tasks;

namespace Opc.Ua.Cloud.Publisher.Interfaces
{
    public interface IBrokerClient
    {
        Task ConnectAsync(bool altBroker = false);

        Task PublishAsync(byte[] payload, string topic = null);

        Task PublishMetadataAsync(byte[] metadata);
    }
}