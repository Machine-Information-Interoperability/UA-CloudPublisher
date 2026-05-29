using System.Threading.Tasks;

namespace Opc.Ua.Cloud.Publisher.Interfaces
{
    public interface IMessagePublisher
    {
        Task<bool> SendMessageAsync(byte[] message, string topic = null);

        Task<bool> SendMetadataAsync(byte[] metadata);

        void ApplyNewClient(IBrokerClient client);

        void ApplyAltClient(IBrokerClient altClient);
    }
}
