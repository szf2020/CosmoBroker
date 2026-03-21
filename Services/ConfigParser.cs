using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;

namespace CosmoBroker.Services;

public class BrokerConfig
{
    public int Port { get; set; } = 4222;
    public int? ClusterPort { get; set; }
    public List<string> Routes { get; } = new();
    public int? JetStreamBatchSize { get; set; }
    public int? JetStreamBatchDelayMs { get; set; }
    public string? RepoConnectionString { get; set; }
    public string? TlsCertPath { get; set; }
    public string? TlsCertPassword { get; set; }
    public bool? TlsClientCertRequired { get; set; }
    public string? AuthType { get; set; }
    public string? AuthUser { get; set; }
    public string? AuthPass { get; set; }
    public string? AuthToken { get; set; }
    public string? JwtOperatorPublicKey { get; set; }
    public string? JwtOperatorJwt { get; set; }
    public List<string> JwtAccountJwts { get; } = new();
    public string? JwtServerNonce { get; set; }
    public string? X509DefaultAccount { get; set; }
    public string? X509SubjectPrefix { get; set; }
}

public static class ConfigParser
{
    public static BrokerConfig Parse(string content)
    {
        var config = new BrokerConfig();
        
        // Very basic recursive-descent style or regex for this simple format
        // port: 4222
        var portMatch = Regex.Match(content, @"port:\s*(\d+)");
        if (portMatch.Success) config.Port = int.Parse(portMatch.Groups[1].Value);

        // cluster { ... }
        var clusterBlock = Regex.Match(content, @"cluster\s*\{([^}]+)\}", RegexOptions.Singleline);
        if (clusterBlock.Success)
        {
            var inner = clusterBlock.Groups[1].Value;
            
            var cPortMatch = Regex.Match(inner, @"port:\s*(\d+)");
            if (cPortMatch.Success) config.ClusterPort = int.Parse(cPortMatch.Groups[1].Value);

            // routes: [ nats://host:port, ... ]
            var routesMatch = Regex.Match(inner, @"routes:\s*\[([^\]]+)\]", RegexOptions.Singleline);
            if (routesMatch.Success)
            {
                var routesStr = routesMatch.Groups[1].Value;
                var parts = routesStr.Split(',', StringSplitOptions.RemoveEmptyEntries);
                foreach (var p in parts)
                {
                    var uri = p.Trim().Trim('"', '\'');
                    if (!string.IsNullOrEmpty(uri)) config.Routes.Add(uri);
                }
            }
        }

        var repoMatch = Regex.Match(content, @"repo:\s*([^\r\n]+)");
        if (repoMatch.Success) config.RepoConnectionString = TrimValue(repoMatch.Groups[1].Value);

        // tls { cert: "path", password: "pwd" }
        var tlsBlock = Regex.Match(content, @"tls\s*\{([^}]+)\}", RegexOptions.Singleline);
        if (tlsBlock.Success)
        {
            var inner = tlsBlock.Groups[1].Value;
            config.TlsCertPath = MatchValue(inner, "cert");
            config.TlsCertPassword = MatchValue(inner, "password");
            var clientCertReq = MatchValue(inner, "client_cert_required");
            if (!string.IsNullOrWhiteSpace(clientCertReq) && bool.TryParse(clientCertReq, out var b))
                config.TlsClientCertRequired = b;
        }

        // auth { type: "simple", user: "u", pass: "p", token: "t", ... }
        var authBlock = Regex.Match(content, @"auth\s*\{([^}]+)\}", RegexOptions.Singleline);
        if (authBlock.Success)
        {
            var inner = authBlock.Groups[1].Value;
            config.AuthType = MatchValue(inner, "type");
            config.AuthUser = MatchValue(inner, "user");
            config.AuthPass = MatchValue(inner, "pass");
            config.AuthToken = MatchValue(inner, "token");
            config.JwtOperatorPublicKey = MatchValue(inner, "jwt_operator_public_key");
            config.JwtOperatorJwt = MatchValue(inner, "jwt_operator_jwt");
            config.JwtServerNonce = MatchValue(inner, "jwt_server_nonce");
            config.X509DefaultAccount = MatchValue(inner, "x509_default_account");
            config.X509SubjectPrefix = MatchValue(inner, "x509_subject_prefix");

            var jwtsMatch = Regex.Match(inner, @"jwt_account_jwts:\s*\[([^\]]+)\]", RegexOptions.Singleline);
            if (jwtsMatch.Success)
            {
                foreach (var part in jwtsMatch.Groups[1].Value.Split(',', StringSplitOptions.RemoveEmptyEntries))
                {
                    var v = TrimValue(part);
                    if (!string.IsNullOrWhiteSpace(v)) config.JwtAccountJwts.Add(v);
                }
            }
            else
            {
                var singleJwt = MatchValue(inner, "jwt_account_jwt");
                if (!string.IsNullOrWhiteSpace(singleJwt)) config.JwtAccountJwts.Add(singleJwt);
            }
        }

        // jetstream { batch_size: 128, batch_delay_ms: 2 }
        var jsBlock = Regex.Match(content, @"jetstream\s*\{([^}]+)\}", RegexOptions.Singleline);
        if (jsBlock.Success)
        {
            var inner = jsBlock.Groups[1].Value;

            var batchSizeMatch = Regex.Match(inner, @"batch_size:\s*(\d+)");
            if (batchSizeMatch.Success) config.JetStreamBatchSize = int.Parse(batchSizeMatch.Groups[1].Value);

            var batchDelayMatch = Regex.Match(inner, @"batch_delay_ms:\s*(\d+)");
            if (batchDelayMatch.Success) config.JetStreamBatchDelayMs = int.Parse(batchDelayMatch.Groups[1].Value);
        }

        return config;
    }

    private static string? MatchValue(string content, string key)
    {
        var m = Regex.Match(content, @$"{Regex.Escape(key)}\s*:\s*([^\r\n,]+)");
        return m.Success ? TrimValue(m.Groups[1].Value) : null;
    }

    private static string? TrimValue(string raw)
    {
        var v = raw.Trim();
        v = v.Trim('"', '\'');
        return v;
    }

    public static BrokerConfig LoadFile(string path)
    {
        if (!File.Exists(path)) return new BrokerConfig();
        return Parse(File.ReadAllText(path));
    }
}
