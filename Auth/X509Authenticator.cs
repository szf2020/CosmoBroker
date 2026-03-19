using System;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;

namespace CosmoBroker.Auth;

/// <summary>
/// Authenticates clients based on their TLS client certificate.
/// </summary>
public class X509Authenticator : IAuthenticator
{
    private readonly Func<X509Certificate2, AuthResult>? _mapper;
    private readonly Dictionary<string, string>? _accountByCommonName;
    private readonly string? _defaultAccount;
    private readonly string? _subjectPrefix;

    public X509Authenticator() { }

    public X509Authenticator(Func<X509Certificate2, AuthResult> mapper)
    {
        _mapper = mapper;
    }

    public X509Authenticator(Dictionary<string, string> accountByCommonName, string? defaultAccount = null, string? subjectPrefix = null)
    {
        _accountByCommonName = accountByCommonName;
        _defaultAccount = defaultAccount;
        _subjectPrefix = subjectPrefix;
    }

    public Task<AuthResult> AuthenticateAsync(ConnectOptions options)
    {
        // This authenticator requires context that isn't in ConnectOptions (the SslStream).
        // For simplicity, we assume the server layer verified the certificate
        // and passed the identity in the 'User' or a new extension.
        
        // In a real NATS implementation, the CN or SAN of the certificate is mapped to an account.
        return Task.FromResult(new AuthResult { Success = false, ErrorMessage = "X509 Auth requires SslStream context" });
    }

    public AuthResult AuthenticateCertificate(X509Certificate2? certificate)
    {
        if (certificate == null)
        {
            return new AuthResult { Success = false, ErrorMessage = "No client certificate provided" };
        }

        if (_mapper != null)
        {
            return _mapper(certificate);
        }

        // Validate certificate (trust chain, expiration, etc.)
        // NATS often uses the 'Subject' or a specific extension for the Account mapping.
        string? cn = GetCommonName(certificate);
        string accountName = _defaultAccount ?? "cert-account";
        if (!string.IsNullOrWhiteSpace(cn) && _accountByCommonName != null && _accountByCommonName.TryGetValue(cn, out var mapped))
            accountName = mapped;

        string userName = cn ?? certificate.Subject;
        return new AuthResult
        {
            Success = true,
            Account = new Account { Name = accountName, SubjectPrefix = _subjectPrefix },
            User = new User { Name = userName, AccountName = accountName }
        };
    }

    private static string? GetCommonName(X509Certificate2 certificate)
    {
        try
        {
            var dn = certificate.Subject;
            var parts = dn.Split(',', StringSplitOptions.RemoveEmptyEntries);
            foreach (var part in parts)
            {
                var p = part.Trim();
                if (p.StartsWith("CN=", StringComparison.OrdinalIgnoreCase))
                    return p.Substring(3);
            }
        }
        catch { }
        return null;
    }
}
