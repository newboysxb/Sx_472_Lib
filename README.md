# Sx Communication

.NET Framework TCP communication library.

## Targets

- `net45`
- `net472`

The .NET Framework 4.7.2 target is also available when referenced by older WinForms applications that share the compatible BCL surface.

## Basic usage

```csharp
using (var client = new TcpClient_sx(new TcpClientOptions
{
    Host = "127.0.0.1",
    Port = 9000
}))
{
    client.DataReceived += (sender, eventArgs) =>
    {
        // For WinForms, dispatch this callback to the UI thread.
        var bytes = eventArgs.Data;
    };

    await client.ConnectAsync();
    await client.SendAsync(new byte[] { 0x01, 0x02 });
}
```

TCP provides a byte stream, not application messages. If a device protocol has frame boundaries, use a separate packet parser before consuming `DataReceived`.

## Build

```bash
dotnet build Sx.Communication.slnx
mono tests/Sx.Communication.IntegrationTest/bin/Debug/net472/Sx.Communication.IntegrationTest.exe
```
