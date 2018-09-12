package py4j;
public class GatewayStart {
    public static void main(String[] args) {
        GatewayStart app = new GatewayStart();
        // app is now the gateway.entry_point
        GatewayServer server = new GatewayServer(app);
        server.start();
    }
}
