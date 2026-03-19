using System;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;
using NATS.NKeys;

namespace CosmoBroker.Tests;

public class JwtChainAuthTests
{
    [Fact]
    public async Task JwtChain_ShouldAuthenticate_WhenOperatorAndAccountRegistered()
    {
        var op = KeyPair.CreatePair(PrefixByte.Operator);
        var acct = KeyPair.CreatePair(PrefixByte.Account);
        var user = KeyPair.CreatePair(PrefixByte.User);

        string opPub = op.GetPublicKey();
        string acctPub = acct.GetPublicKey();
        string userPub = user.GetPublicKey();

        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        string operatorJwt = BuildJwt(op, new {
            iss = opPub,
            sub = opPub,
            iat = now,
            exp = now + 3600
        });

        string accountJwt = BuildJwt(op, new {
            iss = opPub,
            sub = acctPub,
            iat = now,
            exp = now + 3600
        });

        string userJwt = BuildJwt(acct, new {
            iss = acctPub,
            sub = userPub,
            iat = now,
            exp = now + 3600
        });

        var auth = new CosmoBroker.Auth.JwtAuthenticator(serverNonce: "nonce", operatorJwt: operatorJwt);
        auth.RegisterAccountJwt(accountJwt);

        var sig = Convert.ToBase64String(Sign(user, Encoding.UTF8.GetBytes("nonce")));
        var result = await auth.AuthenticateAsync(new CosmoBroker.Auth.ConnectOptions {
            Jwt = userJwt,
            Sig = sig
        });

        Assert.True(result.Success);
        Assert.Equal(acctPub, result.Account?.Name);
        Assert.Equal(userPub, result.User?.Name);
    }

    [Fact]
    public async Task JwtChain_ShouldFail_WhenAccountJwtMissing()
    {
        var op = KeyPair.CreatePair(PrefixByte.Operator);
        var acct = KeyPair.CreatePair(PrefixByte.Account);
        var user = KeyPair.CreatePair(PrefixByte.User);

        string opPub = op.GetPublicKey();
        string acctPub = acct.GetPublicKey();
        string userPub = user.GetPublicKey();

        var now = DateTimeOffset.UtcNow.ToUnixTimeSeconds();
        string operatorJwt = BuildJwt(op, new {
            iss = opPub,
            sub = opPub,
            iat = now,
            exp = now + 3600
        });

        string userJwt = BuildJwt(acct, new {
            iss = acctPub,
            sub = userPub,
            iat = now,
            exp = now + 3600
        });

        var auth = new CosmoBroker.Auth.JwtAuthenticator(serverNonce: "nonce", operatorJwt: operatorJwt);
        var sig = Convert.ToBase64String(Sign(user, Encoding.UTF8.GetBytes("nonce")));

        var result = await auth.AuthenticateAsync(new CosmoBroker.Auth.ConnectOptions {
            Jwt = userJwt,
            Sig = sig
        });

        Assert.False(result.Success);
        Assert.Contains("Account JWT not registered", result.ErrorMessage ?? string.Empty);
    }

    private static string BuildJwt(KeyPair signer, object payload)
    {
        var headerJson = JsonSerializer.Serialize(new { typ = "JWT", alg = "ed25519-nkey" });
        var payloadJson = JsonSerializer.Serialize(payload);
        var header = Base64UrlEncode(Encoding.UTF8.GetBytes(headerJson));
        var body = Base64UrlEncode(Encoding.UTF8.GetBytes(payloadJson));
        var signingInput = $"{header}.{body}";
        var sig = Sign(signer, Encoding.UTF8.GetBytes(signingInput));
        var sigB64 = Base64UrlEncode(sig);
        return $"{signingInput}.{sigB64}";
    }

    private static string Base64UrlEncode(byte[] data)
    {
        return Convert.ToBase64String(data).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }

    private static byte[] Sign(KeyPair signer, byte[] data)
    {
        var sig = new byte[64];
        signer.Sign(data, sig);
        return sig;
    }
}
