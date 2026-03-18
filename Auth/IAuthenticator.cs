using System.Threading.Tasks;

namespace CosmoBroker.Auth;

public interface IAuthenticator
{
    /// <summary>
    /// Authenticates a client based on provided options from the CONNECT command.
    /// </summary>
    Task<bool> AuthenticateAsync(ConnectOptions options);
}
