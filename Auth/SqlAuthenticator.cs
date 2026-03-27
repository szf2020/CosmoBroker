using System;
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

    public async Task<AuthResult> AuthenticateAsync(ConnectOptions options)
    {
        bool success = false;
        string? resolvedUser = options.User;
        if (!string.IsNullOrEmpty(options.AuthToken))
        {
            success = await _repo.ValidateTokenAsync(options.AuthToken);
            if (success)
            {
                resolvedUser = await _repo.GetUserByTokenAsync(options.AuthToken) ?? resolvedUser;
            }
        }
        else if (!string.IsNullOrEmpty(options.User))
        {
            success = await _repo.ValidateUserAsync(options.User, options.Pass ?? string.Empty);
        }

        if (!success)
        {
            return new AuthResult { Success = false, ErrorMessage = "SQL Authentication failed" };
        }

        var userName = resolvedUser ?? "sql-user";
        var perms = await _repo.GetUserPermissionsAsync(userName);
        var rabbitPerms = await _repo.GetRabbitPermissionsAsync(userName);

        var account = new Account
        {
            Name = perms?.AccountName ?? "sql-account",
            SubjectPrefix = perms?.SubjectPrefix ?? resolvedUser
        };
        if (perms != null)
        {
            account.AllowPublish.AddRange(perms.AllowPublish);
            account.DenyPublish.AddRange(perms.DenyPublish);
            account.AllowSubscribe.AddRange(perms.AllowSubscribe);
            account.DenySubscribe.AddRange(perms.DenySubscribe);
        }
        if (rabbitPerms != null)
        {
            account.RabbitPermissionsConfigured = true;
            account.DefaultRabbitVhost = rabbitPerms.DefaultVhost;
            account.RabbitAllowedVhosts.AddRange(rabbitPerms.AllowedVhosts);
            if (account.RabbitAllowedVhosts.Count == 0)
                account.RabbitAllowedVhosts.Add(rabbitPerms.DefaultVhost);
            account.RabbitConfigure.AddRange(rabbitPerms.ConfigurePatterns);
            account.RabbitWrite.AddRange(rabbitPerms.WritePatterns);
            account.RabbitRead.AddRange(rabbitPerms.ReadPatterns);
        }

        var user = new User { Name = userName, AccountName = account.Name };

        return new AuthResult { Success = true, Account = account, User = user };
    }
}
