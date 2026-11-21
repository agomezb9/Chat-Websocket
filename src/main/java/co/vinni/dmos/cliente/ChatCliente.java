package co.vinni.dmos.cliente;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;

/**
 * Cliente de consola para el chat por salas (general, musica, deportes).
 *
 * Usa el cliente WebSocket que trae Java (java.net.http), no necesita librerías extra.
 *
 * Ejecutar (con el servidor NetworkApplication ya corriendo):
 *   java co.vinni.dmos.cliente.ChatCliente                 -> pregunta nombre y sala
 *   java co.vinni.dmos.cliente.ChatCliente Ana musica      -> entra directo
 *   java co.vinni.dmos.cliente.ChatCliente Ana musica ws://192.168.1.10:8862/chat
 */
public class ChatCliente {

    private static final String SERVIDOR_POR_DEFECTO = "ws://localhost:8862/chat";

    public static void main(String[] args) throws Exception {
        Scanner teclado = new Scanner(System.in, StandardCharsets.UTF_8);

        String usuario = args.length > 0 ? args[0] : preguntar(teclado, "Tu nombre: ");
        String sala = args.length > 1 ? args[1] : elegirSala(teclado);
        String servidor = args.length > 2 ? args[2] : SERVIDOR_POR_DEFECTO;

        URI uri = URI.create(servidor
                + "?usuario=" + URLEncoder.encode(usuario, StandardCharsets.UTF_8)
                + "&sala=" + URLEncoder.encode(sala, StandardCharsets.UTF_8));

        CountDownLatch cerrado = new CountDownLatch(1);

        System.out.println("Conectando a " + servidor + " ...");
        WebSocket ws;
        try {
            ws = HttpClient.newHttpClient()
                    .newWebSocketBuilder()
                    .buildAsync(uri, new Escucha(cerrado))
                    .join();
        } catch (Exception e) {
            System.out.println("❌ No se pudo conectar. ¿Está corriendo el servidor? (" + e.getMessage() + ")");
            return;
        }

        imprimirMenu();

        // Hilo principal: lee lo que escribe el usuario y lo envía
        while (cerrado.getCount() > 0 && teclado.hasNextLine()) {
            String linea = teclado.nextLine().trim();
            if (linea.isEmpty()) {
                continue;
            }
            String aEnviar = traducirAtajo(linea);
            if (aEnviar == null) {          // /salir
                break;
            }
            if (aEnviar.equals("/menu")) {
                imprimirMenu();
                continue;
            }
            // join(): espera a que termine el envío antes de mandar el siguiente
            ws.sendText(aEnviar, true).join();
        }

        if (!ws.isOutputClosed()) {
            ws.sendClose(WebSocket.NORMAL_CLOSURE, "Adiós").join();
        }
        System.out.println("👋 Desconectado del chat.");
    }

    /** Permite cambiar de sala escribiendo solo 1, 2 o 3. */
    private static String traducirAtajo(String linea) {
        return switch (linea.toLowerCase()) {
            case "1" -> "/unirse general";
            case "2" -> "/unirse musica";
            case "3" -> "/unirse deportes";
            case "/salir", "salir" -> null;
            case "/menu", "menu" -> "/menu";
            default -> linea;
        };
    }

    private static void imprimirMenu() {
        System.out.println("""
                +-------------------- CHAT POR SALAS --------------------+
                |  1 = General    2 = Musica    3 = Deportes             |
                |  /salas      ver salas y cuantos hay en cada una       |
                |  /usuarios   ver quien esta en tu sala                 |
                |  /nombre X   cambiar tu nombre                         |
                |  /menu       ver este menu      /salir   desconectarse |
                |  Cualquier otro texto se envia a tu sala               |
                +--------------------------------------------------------+""");
    }

    private static String elegirSala(Scanner teclado) {
        System.out.println("Salas: 1) General  2) Música  3) Deportes");
        while (true) {
            String op = preguntar(teclado, "Elige sala (1-3): ");
            switch (op) {
                case "1": return "general";
                case "2": return "musica";
                case "3": return "deportes";
                default: System.out.println("Opción no válida.");
            }
        }
    }

    private static String preguntar(Scanner teclado, String texto) {
        System.out.print(texto);
        return teclado.nextLine().trim();
    }

    /** Recibe los mensajes del servidor y los muestra en consola. */
    private static class Escucha implements WebSocket.Listener {
        private final CountDownLatch cerrado;
        private final StringBuilder buffer = new StringBuilder();

        Escucha(CountDownLatch cerrado) {
            this.cerrado = cerrado;
        }

        @Override
        public void onOpen(WebSocket ws) {
            System.out.println("🟢 Conectado.");
            ws.request(1);
        }

        @Override
        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean ultimo) {
            // Un mensaje largo puede llegar en varias partes; se juntan hasta la última
            buffer.append(data);
            if (ultimo) {
                System.out.println(buffer);
                buffer.setLength(0);
            }
            ws.request(1);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletionStage<?> onClose(WebSocket ws, int codigo, String razon) {
            System.out.println("🔴 El servidor cerró la conexión (" + codigo + ")");
            cerrado.countDown();
            return null;
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            System.out.println("❌ Error de conexión: " + error.getMessage());
            cerrado.countDown();
        }
    }
}
