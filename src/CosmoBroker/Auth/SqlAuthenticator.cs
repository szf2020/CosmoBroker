using System.Threading.Tasks;
using CosmoBroker.Persistence;

namespace CosmoBroker.Auth;

public class SqlAuthenticator : IAuthenticator
{
    private readonly MessageRepository _repo;

    public SqlAuthenticator(MessageRepository repo)
    {
        _repo = repo ?? throw new ArgumentNullException(nameof(repo));
    }

    public async Task<bool> AuthenticateAsync(ConnectOptions options)
    {
        if (!string.IsNullOrEmpty(options.AuthToken))
        {
            return await _repo.ValidateTokenAsync(options.AuthToken);
        }

        if (!string.IsNullOrEmpty(options.User))
        {
            return await _repo.ValidateUserAsync(options.User, options.Pass ?? string.Empty);
        }

        return false; // Auth is required but no credentials provided
    }
}
