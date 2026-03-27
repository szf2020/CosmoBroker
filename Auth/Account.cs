using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace CosmoBroker.Auth;

public class Account
{
    public string Name { get; set; } = string.Empty;
    
    // Prefix for total isolation. If null, account shares global space.
    // NATS accounts are isolated by default.
    public string? SubjectPrefix { get; set; }

    // Fine-grained permissions
    public List<string> AllowPublish { get; } = new();
    public List<string> DenyPublish { get; } = new();
    
    public List<string> AllowSubscribe { get; } = new();
    public List<string> DenySubscribe { get; } = new();

    // Mapping for Imports/Exports (advanced)
    // Key: Local subject, Value: Remote (account.subject)
    public Dictionary<string, string> Imports { get; } = new();
    public Dictionary<string, string> Exports { get; } = new();

    public Services.SubjectMapper Mappings { get; } = new();

    public string DefaultRabbitVhost { get; set; } = "/";
    public bool RabbitPermissionsConfigured { get; set; }
    public List<string> RabbitAllowedVhosts { get; } = new();
    public List<string> RabbitConfigure { get; } = new();
    public List<string> RabbitWrite { get; } = new();
    public List<string> RabbitRead { get; } = new();

    public bool CanAccessRabbitVhost(string? vhost)
    {
        var resolved = string.IsNullOrWhiteSpace(vhost) ? DefaultRabbitVhost : vhost!;
        if (!RabbitPermissionsConfigured) return true;
        if (RabbitAllowedVhosts.Count == 0) return false;
        return MatchesAny(RabbitAllowedVhosts, resolved);
    }

    public bool CanRabbitConfigure(string? vhost, string resource)
        => HasRabbitPermission(vhost, resource, RabbitConfigure);

    public bool CanRabbitWrite(string? vhost, string resource)
        => HasRabbitPermission(vhost, resource, RabbitWrite);

    public bool CanRabbitRead(string? vhost, string resource)
        => HasRabbitPermission(vhost, resource, RabbitRead);

    private bool HasRabbitPermission(string? vhost, string resource, List<string> patterns)
    {
        if (!CanAccessRabbitVhost(vhost)) return false;
        if (!RabbitPermissionsConfigured) return true;
        if (patterns.Count == 0) return false;
        return MatchesAny(patterns, resource);
    }

    private static bool MatchesAny(IEnumerable<string> patterns, string value)
    {
        foreach (var pattern in patterns)
        {
            if (GlobMatches(pattern, value))
                return true;
        }

        return false;
    }

    private static bool GlobMatches(string pattern, string value)
    {
        if (string.IsNullOrWhiteSpace(pattern))
            return false;
        if (pattern == "*")
            return true;
        if (!pattern.Contains('*'))
            return string.Equals(pattern, value, System.StringComparison.OrdinalIgnoreCase);

        var regex = "^" + Regex.Escape(pattern).Replace("\\*", ".*") + "$";
        return Regex.IsMatch(value, regex, RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    }
}

public class User
{
    public string Name { get; set; } = string.Empty;
    public string? AccountName { get; set; }
    
    // Permissions can also be user-level, overriding or adding to account-level
    public List<string> AllowPublish { get; } = new();
    public List<string> AllowSubscribe { get; } = new();
}

public class AuthResult
{
    public bool Success { get; set; }
    public Account? Account { get; set; }
    public User? User { get; set; }
    public string? ErrorMessage { get; set; }
}
