using System.Threading.Tasks;

namespace CosmoBroker.Auth;

public class SimpleAuthenticator : IAuthenticator
{
    private readonly string? _user;
    private readonly string? _pass;
    private readonly string? _token;

    public SimpleAuthenticator(string? user = null, string? pass = null, string? token = null)
    {
        _user = user;
        _pass = pass;
        _token = token;
    }

    public Task<bool> AuthenticateAsync(ConnectOptions options)
    {
        if (!string.IsNullOrEmpty(_token))
        {
            return Task.FromResult(options.AuthToken == _token);
        }

        if (!string.IsNullOrEmpty(_user))
        {
            return Task.FromResult(options.User == _user && options.Pass == _pass);
        }

        return Task.FromResult(true); // No auth required
    }
}
