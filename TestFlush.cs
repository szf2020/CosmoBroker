using System;
using System.Net;
using System.Net.Sockets;
using System.IO;
using System.Threading.Tasks;

class Program {
    static async Task Main() {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        
        var clientTask = Task.Run(async () => {
            using var client = new TcpClient();
            await client.ConnectAsync(IPAddress.Loopback, port);
            using var stream = client.GetStream();
            await stream.WriteAsync(new byte[] { 1, 2, 3 });
            try {
                await stream.FlushAsync();
                Console.WriteLine("FlushAsync supported");
            } catch (Exception ex) {
                Console.WriteLine("FlushAsync NOT supported: " + ex.Message);
            }
        });

        using var server = await listener.AcceptTcpClientAsync();
        await clientTask;
        listener.Stop();
    }
}
