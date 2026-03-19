using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Xunit;
using CosmoBroker.Auth;

namespace CosmoBroker.Tests;

public class X509AuthTests
{
    [Fact]
    public void X509Authenticator_MapsCommonNameToAccount()
    {
        var cert = CreateSelfSignedCert("CN=alice");
        var map = new System.Collections.Generic.Dictionary<string, string> { ["alice"] = "acct-alice" };
        var auth = new X509Authenticator(map, defaultAccount: "default");

        var result = auth.AuthenticateCertificate(cert);

        Assert.True(result.Success);
        Assert.Equal("acct-alice", result.Account?.Name);
        Assert.Equal("alice", result.User?.Name);
    }

    private static X509Certificate2 CreateSelfSignedCert(string subject)
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest(subject, rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        return req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
    }
}

