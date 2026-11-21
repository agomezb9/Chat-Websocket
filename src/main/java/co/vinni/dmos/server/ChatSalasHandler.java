package co.vinni.dmos.server;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.ConcurrentWebSocketSessionDecorator;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.Normalizer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Chat con salas públicas (GENERAL, MUSICA, DEPORTES) y salas privadas (priv-xxxx).
 *
 * Conexión:  ws://localhost:8862/chat?usuario=Ana&sala=musica
 *            Parámetros (todos opcionales):
 *              usuario  nombre; si falta se asigna "Invitado-xxxx"
 *              sala     general | musica | deportes | priv-<código>; por defecto "general"
 *              clave    para salas privadas: la fija quien la crea y los demás deben enviarla
 *              formato  "json" para recibir objetos JSON (cliente web); sin él se recibe texto plano
 *              cid      identificador estable del cliente (el cliente web lo guarda en su pestaña);
 *                       así, tras recargar la página, sus mensajes anteriores se reconocen como suyos
 *
 * Historial: cada sala guarda en memoria sus últimos mensajes (texto y voz) y se los envía a quien
 * entra, también cuando alguien vuelve a una sala o recarga la página.
 *
 * Salas privadas: cualquier nombre "priv-" + 3 a 32 letras/números se crea al entrar el primero y
 * no aparece en /salas. Si queda vacía se conserva (con su historial y su clave) unos minutos, para
 * poder volver a entrar, y después se borra.
 *
 * Lo que envía el cliente (texto plano o JSON):
 *   hola                                   -> mensaje de texto a su sala
 *   {"tipo":"texto","contenido":"hola"}    -> igual que lo anterior
 *   {"tipo":"audio","contenido":"data:audio/webm;base64,..."}  -> mensaje de voz
 *   {"tipo":"ping"}                        -> el servidor responde {"tipo":"pong"}
 *
 * Comandos (dentro de un mensaje de texto):
 *   /salas              -> lista las salas públicas y cuántos usuarios hay en cada una
 *   /unirse <sala> [clave]  -> cambia de sala
 *   /usuarios           -> lista los usuarios de la sala actual
 *   /nombre <nuevo>     -> cambia el nombre de usuario
 *   /ayuda              -> muestra los comandos
 *   PING                -> responde PONG
 */
public class ChatSalasHandler extends TextWebSocketHandler {

    public static final List<String> SALAS = List.of("general", "musica", "deportes");

    /** Límite de tamaño de mensajes WebSocket (2 MB). WebSocketConfig usa el mismo valor. */
    public static final int LIMITE_BUFFER = 2 * 1024 * 1024;

    /** Máximo de caracteres de un audio en base64 (deja margen bajo el límite de 2 MB). */
    private static final int MAX_AUDIO_CHARS = 1_500_000;

    /** Historial de cada sala: como máximo 200 mensajes y 1 millón de caracteres (unas 3 notas de voz). */
    private static final int HISTORIAL_MAX_MENSAJES = 200;
    private static final int HISTORIAL_MAX_CHARS = 1_000_000;

    /** Cuánto se conserva una sala privada después de quedar vacía. */
    private static final long PRIVADA_VACIA_TTL_MS = 30 * 60 * 1000L;

    private static final String SALA_POR_DEFECTO = "general";
    private static final String PREFIJO_PRIVADA = "priv-";
    private static final Pattern SALA_PRIVADA = Pattern.compile("priv-[a-z0-9]{3,32}");
    private static final Pattern CID_VALIDO = Pattern.compile("[A-Za-z0-9_-]{8,40}");

    private static final String ATTR_SALA = "sala";
    private static final String ATTR_USUARIO = "usuario";
    private static final String ATTR_JSON = "json";
    private static final String ATTR_CID = "cid";

    private static final DateTimeFormatter HORA =
            DateTimeFormatter.ofPattern("HH:mm").withZone(ZoneId.systemDefault());

    private static final ObjectMapper JSON = new ObjectMapper();

    /**
     * Lo que se envía a los clientes web (formato=json). Los clientes de texto plano reciben
     * la misma información convertida a una línea de texto.
     * tipo: texto | audio | sistema | pong.   evento (solo en "sistema"): bienvenida | sala.
     * id: identificador del cliente que lo envió (el cid), para saber cuáles mensajes son míos.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Msg(String tipo, String evento, String id, String usuario,
                      String sala, Long ts, String contenido) {
    }

    /** Los mensajes anteriores de una sala, enviados de una sola vez al cliente web al entrar. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record PaqueteHistorial(String tipo, String sala, List<Msg> mensajes) {
    }

    /** Últimos mensajes de una sala, con tope de cantidad y de tamaño. */
    private static final class Historial {
        private final ArrayDeque<Msg> mensajes = new ArrayDeque<>();
        private int caracteres = 0;

        synchronized void agregar(Msg m) {
            int peso = peso(m);
            if (peso > HISTORIAL_MAX_CHARS) {
                return; // un audio enorme se entrega a la sala, pero no se guarda
            }
            mensajes.addLast(m);
            caracteres += peso;
            while (mensajes.size() > HISTORIAL_MAX_MENSAJES || caracteres > HISTORIAL_MAX_CHARS) {
                caracteres -= peso(mensajes.removeFirst());
            }
        }

        synchronized List<Msg> copia() {
            return new ArrayList<>(mensajes);
        }

        private static int peso(Msg m) {
            return 100 + (m.contenido() == null ? 0 : m.contenido().length());
        }
    }

    /** Una sala: quién está dentro, sus mensajes anteriores y (si es privada) su clave. */
    private static final class Sala {
        final Set<WebSocketSession> miembros = ConcurrentHashMap.newKeySet();
        final Historial historial = new Historial();
        final String clave;           // "" si no tiene
        volatile long vacioDesde = 0; // 0 mientras haya gente; si no, la hora (ms) en que quedó vacía

        Sala(String clave) {
            this.clave = clave;
        }
    }

    private final Map<String, Sala> salas = new ConcurrentHashMap<>();

    /** id de sesión -> sesión "segura" para enviar desde varios hilos a la vez */
    private final Map<String, WebSocketSession> sesiones = new ConcurrentHashMap<>();

    /** Protege crear/borrar salas privadas y entrar/salir de ellas. */
    private final Object lock = new Object();

    public ChatSalasHandler() {
        for (String sala : SALAS) {
            salas.put(sala, new Sala(""));
        }
    }

    // ------------------------------------------------------------------
    // Ciclo de vida de la conexión
    // ------------------------------------------------------------------

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        // Spring no permite enviar mensajes a la misma sesión desde dos hilos a la vez;
        // este decorador lo hace seguro (útil cuando varios usuarios escriben al mismo tiempo).
        WebSocketSession segura = new ConcurrentWebSocketSessionDecorator(session, 10_000, LIMITE_BUFFER);
        sesiones.put(session.getId(), segura);

        Map<String, String> params = leerParametros(session);
        String usuario = params.getOrDefault("usuario", "").trim();
        if (usuario.isEmpty()) {
            usuario = "Invitado-" + session.getId().substring(0, 4);
        }
        String cid = params.getOrDefault("cid", "");
        session.getAttributes().put(ATTR_USUARIO, usuario);
        session.getAttributes().put(ATTR_CID, CID_VALIDO.matcher(cid).matches() ? cid : session.getId());
        session.getAttributes().put(ATTR_JSON, "json".equalsIgnoreCase(params.get("formato")));

        String sala = normalizarSala(params.get("sala"));
        if (sala == null) {
            sala = SALA_POR_DEFECTO;
        }

        System.out.println("🔌 [CHAT] Conectado: " + usuario + " (" + session.getId() + ")");
        entregar(segura, new Msg("sistema", "bienvenida", cidDe(session), usuario, null, ahora(),
                "Bienvenido/a " + usuario + ". Salas públicas: " + String.join(", ", SALAS)
                        + ". Escribe /ayuda para ver los comandos."));

        if (!entrar(session, segura, sala, params.getOrDefault("clave", ""))) {
            aviso(segura, "🔒 Clave incorrecta para esa sala privada.");
            session.close(CloseStatus.POLICY_VIOLATION.withReason("Clave incorrecta"));
            return;
        }
        anunciarEntrada(session, sala);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        WebSocketSession segura = sesiones.get(session.getId());
        String tipo = "texto";
        String contenido = message.getPayload();

        if (contenido.startsWith("{")) {
            try {
                JsonNode json = JSON.readTree(contenido);
                if (json.isObject() && json.has("tipo")) {
                    tipo = json.path("tipo").asText("").toLowerCase();
                    contenido = json.path("contenido").asText("");
                }
            } catch (IOException e) {
                // No era JSON válido: se trata como texto plano
            }
        }

        switch (tipo) {
            case "texto" -> procesarTexto(session, segura, contenido.trim());
            case "audio" -> procesarAudio(session, segura, contenido);
            case "ping" -> entregar(segura, new Msg("pong", null, null, null, null, null, null));
            default -> aviso(segura, "⚠️ Tipo de mensaje no soportado: " + tipo);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        String sala = salaDe(session);
        String usuario = usuarioDe(session);
        WebSocketSession segura = sesiones.remove(session.getId());
        if (sala != null && segura != null) {
            quitar(segura, sala);
            difundir(sala, avisoMsg("🔴 " + usuario + " salió de la sala " + sala.toUpperCase()));
        }
        System.out.println("❌ [CHAT] Desconectado: " + usuario + " (" + session.getId() + ")");
    }

    // ------------------------------------------------------------------
    // Mensajes de texto, audio y comandos
    // ------------------------------------------------------------------

    private void procesarTexto(WebSocketSession session, WebSocketSession segura, String texto) {
        if (texto.isEmpty()) {
            return;
        }
        if ("PING".equalsIgnoreCase(texto)) {
            entregar(segura, new Msg("pong", null, null, null, null, null, null));
            return;
        }
        if (texto.startsWith("/")) {
            procesarComando(session, segura, texto);
            return;
        }

        String sala = salaDe(session);
        if (sala == null) {
            return;
        }
        // Lo que se dice en salas privadas no se escribe en la consola del servidor
        if (!esPrivada(sala)) {
            System.out.println("💬 [" + sala + "] " + usuarioDe(session) + ": " + texto);
        }
        publicar(sala, mensaje("texto", session, sala, texto));
    }

    private void procesarAudio(WebSocketSession session, WebSocketSession segura, String datos) {
        String sala = salaDe(session);
        if (sala == null) {
            return;
        }
        if (!datos.startsWith("data:audio/")) {
            aviso(segura, "⚠️ Audio no válido.");
            return;
        }
        if (datos.length() > MAX_AUDIO_CHARS) {
            aviso(segura, "⚠️ El audio es demasiado largo. Graba un mensaje más corto.");
            return;
        }
        if (!esPrivada(sala)) {
            System.out.println("🎤 [" + sala + "] " + usuarioDe(session) + ": mensaje de voz (" + datos.length() / 1024 + " KB)");
        }
        publicar(sala, mensaje("audio", session, sala, datos));
    }

    private void procesarComando(WebSocketSession session, WebSocketSession segura, String texto) {
        String[] partes = texto.split("\\s+", 2);
        String comando = partes[0].toLowerCase();
        String argumento = partes.length > 1 ? partes[1].trim() : "";

        switch (comando) {
            case "/salas" -> {
                String actual = salaDe(session);
                String lista = SALAS.stream()
                        .map(s -> s.toUpperCase() + " (" + miembros(s).size() + ")"
                                + (s.equals(actual) ? " ← estás aquí" : ""))
                        .collect(Collectors.joining(" | "));
                if (esPrivada(actual)) {
                    lista += " | 🔒 estás en una sala privada";
                }
                aviso(segura, "Salas: " + lista);
            }
            case "/unirse" -> unirse(session, segura, argumento);
            case "/usuarios" -> {
                String usuarios = miembros(salaDe(session)).stream()
                        .map(this::usuarioDe)
                        .sorted()
                        .collect(Collectors.joining(", "));
                aviso(segura, "Usuarios en " + salaDe(session).toUpperCase() + ": " + usuarios);
            }
            case "/nombre" -> {
                if (argumento.isEmpty()) {
                    aviso(segura, "⚠️ Uso: /nombre <nuevo nombre>");
                } else {
                    String anterior = usuarioDe(session);
                    session.getAttributes().put(ATTR_USUARIO, argumento);
                    difundir(salaDe(session), avisoMsg("✏️ " + anterior + " ahora se llama " + argumento));
                }
            }
            case "/ayuda" -> aviso(segura, """
                    Comandos:
                      /salas                  ver salas públicas y usuarios conectados
                      /unirse <sala> [clave]  cambiar a general, musica, deportes o priv-<código>
                      /usuarios               ver quién está en tu sala
                      /nombre <nombre>        cambiar tu nombre
                      cualquier texto         enviar mensaje a tu sala""");
            default -> aviso(segura, "⚠️ Comando desconocido: " + comando + ". Escribe /ayuda");
        }
    }

    private void unirse(WebSocketSession session, WebSocketSession segura, String argumento) {
        String[] partes = argumento.split("\\s+", 2);
        String nueva = normalizarSala(partes[0]);
        String clave = partes.length > 1 ? partes[1].trim() : "";
        String actual = salaDe(session);

        if (nueva == null) {
            aviso(segura, "⚠️ Sala no válida. Usa: /unirse general | musica | deportes | priv-<código> [clave]");
        } else if (nueva.equals(actual)) {
            aviso(segura, "Ya estás en la sala " + nueva.toUpperCase());
        } else if (!entrar(session, segura, nueva, clave)) {
            // Si la clave falla, el usuario sigue en la sala donde estaba
            aviso(segura, "🔒 Clave incorrecta para esa sala privada.");
        } else {
            if (actual != null) {
                quitar(segura, actual);
                difundir(actual, avisoMsg("🔴 " + usuarioDe(session) + " salió de la sala " + actual.toUpperCase()));
            }
            anunciarEntrada(session, nueva);
        }
    }

    // ------------------------------------------------------------------
    // Manejo de salas
    // ------------------------------------------------------------------

    /**
     * Mete la sesión en la sala y le envía el aviso de entrada y los mensajes anteriores. En una
     * sala privada que no existe la crea y guarda la clave; si ya existe y tiene clave, solo deja
     * entrar si coincide. Devuelve false si se rechaza.
     */
    private boolean entrar(WebSocketSession session, WebSocketSession segura, String sala, String clave) {
        Sala s;
        synchronized (lock) {
            borrarPrivadasVencidas();
            s = salas.get(sala);
            if (esPrivada(sala)) {
                if (s == null) {
                    s = new Sala(clave);
                    salas.put(sala, s);
                } else if (!claveCorrecta(s.clave, clave)) {
                    return false;
                }
            }
            s.vacioDesde = 0; // marcada en uso: nadie la borra mientras se termina de entrar
        }
        // Mismo candado que usa publicar(): quien entra recibe primero los mensajes anteriores y después
        // los nuevos, en orden, sin repetir ninguno ni perder ninguno. El envío no se hace con el
        // candado general tomado, para que un cliente lento no frene a los demás.
        synchronized (s.historial) {
            s.miembros.add(segura);
            session.getAttributes().put(ATTR_SALA, sala);
            entregar(segura, new Msg("sistema", "sala", null, null, sala, ahora(),
                    "✅ Entraste a la sala " + sala.toUpperCase()));
            enviarHistorial(segura, sala, s.historial.copia());
        }
        return true;
    }

    /** Saca la sesión de la sala; una sala privada que queda vacía empieza a contar su tiempo de vida. */
    private void quitar(WebSocketSession segura, String sala) {
        synchronized (lock) {
            Sala s = salas.get(sala);
            if (s == null) {
                return;
            }
            s.miembros.remove(segura);
            if (esPrivada(sala) && s.miembros.isEmpty()) {
                s.vacioDesde = System.currentTimeMillis();
            }
            borrarPrivadasVencidas();
        }
    }

    /** Borra las salas privadas que llevan vacías más del tiempo permitido. Llamar con el candado tomado. */
    private void borrarPrivadasVencidas() {
        long limite = System.currentTimeMillis() - PRIVADA_VACIA_TTL_MS;
        salas.entrySet().removeIf(e -> esPrivada(e.getKey())
                && e.getValue().miembros.isEmpty()
                && e.getValue().vacioDesde > 0
                && e.getValue().vacioDesde < limite);
    }

    /** Avisa a los demás de la sala que alguien entró. */
    private void anunciarEntrada(WebSocketSession session, String sala) {
        difundirExcepto(sala, avisoMsg("🟢 " + usuarioDe(session) + " entró a la sala " + sala.toUpperCase()),
                session.getId());
        System.out.println("➡️ [CHAT] " + usuarioDe(session) + " entró a " + (esPrivada(sala) ? "una sala privada" : sala));
    }

    private boolean claveCorrecta(String esperada, String recibida) {
        if (esperada.isEmpty()) {
            return true;
        }
        return MessageDigest.isEqual(esperada.getBytes(StandardCharsets.UTF_8),
                recibida.getBytes(StandardCharsets.UTF_8));
    }

    private Set<WebSocketSession> miembros(String sala) {
        Sala s = sala == null ? null : salas.get(sala);
        return s == null ? Set.of() : s.miembros;
    }

    /** Guarda un mensaje (texto o voz) en el historial de la sala y lo envía a quienes están dentro. */
    private void publicar(String sala, Msg msg) {
        Sala s = salas.get(sala);
        if (s == null) {
            return;
        }
        synchronized (s.historial) {
            s.historial.agregar(msg);
            difundir(sala, msg);
        }
    }

    private void difundir(String sala, Msg mensaje) {
        difundirExcepto(sala, mensaje, null);
    }

    private void difundirExcepto(String sala, Msg mensaje, String idExcluido) {
        for (WebSocketSession s : miembros(sala)) {
            if (!s.getId().equals(idExcluido)) {
                entregar(s, mensaje);
            }
        }
    }

    // ------------------------------------------------------------------
    // Envío
    // ------------------------------------------------------------------

    /** Envía un mensaje a un usuario: JSON para el cliente web, una línea de texto para los demás. */
    private void entregar(WebSocketSession destino, Msg msg) {
        enviar(destino, msg, comoTexto(msg));
    }

    /**
     * Envía los mensajes anteriores de la sala. El cliente web los recibe en un solo paquete;
     * los clientes de texto plano, como una línea por mensaje entre dos avisos.
     */
    private void enviarHistorial(WebSocketSession destino, String sala, List<Msg> anteriores) {
        if (anteriores.isEmpty()) {
            return;
        }
        if (esJson(destino)) {
            enviar(destino, new PaqueteHistorial("historial", sala, anteriores), null);
        } else {
            aviso(destino, "📜 Mensajes anteriores de la sala:");
            anteriores.forEach(m -> entregar(destino, m));
            aviso(destino, "📜 — fin de los mensajes anteriores —");
        }
    }

    private void enviar(WebSocketSession destino, Object comoJson, String comoTexto) {
        if (destino == null || !destino.isOpen()) {
            return;
        }
        try {
            String salida = esJson(destino) ? JSON.writeValueAsString(comoJson) : comoTexto;
            destino.sendMessage(new TextMessage(salida));
        } catch (Exception e) {
            // Un destinatario lento o caído no debe impedir que el resto reciba el mensaje
            System.err.println("Error enviando a " + destino.getId() + ": " + e.getMessage());
        }
    }

    private String comoTexto(Msg m) {
        String etiqueta = m.sala() == null ? "" : "[" + m.sala().toUpperCase() + "] ";
        String hora = m.ts() == null ? "" : HORA.format(Instant.ofEpochMilli(m.ts())) + " ";
        return switch (m.tipo()) {
            case "texto" -> etiqueta + hora + m.usuario() + ": " + m.contenido();
            case "audio" -> etiqueta + hora + m.usuario() + ": 🎤 envió un mensaje de voz (solo se escucha en el cliente web)";
            case "pong" -> "PONG";
            default -> m.contenido();
        };
    }

    private Msg mensaje(String tipo, WebSocketSession session, String sala, String contenido) {
        return new Msg(tipo, null, cidDe(session), usuarioDe(session), sala, ahora(), contenido);
    }

    private Msg avisoMsg(String contenido) {
        return new Msg("sistema", null, null, null, null, ahora(), contenido);
    }

    private void aviso(WebSocketSession destino, String contenido) {
        entregar(destino, avisoMsg(contenido));
    }

    private Long ahora() {
        return System.currentTimeMillis();
    }

    // ------------------------------------------------------------------
    // Utilidades
    // ------------------------------------------------------------------

    /**
     * Acepta "Música", "MUSICA", " deportes ", "PRIV-ab12" etc. Devuelve el nombre en minúsculas
     * y sin tildes, o null si no es una sala válida (pública o privada).
     */
    static String normalizarSala(String sala) {
        if (sala == null) {
            return null;
        }
        String limpia = Normalizer.normalize(sala.trim().toLowerCase(), Normalizer.Form.NFD)
                .replaceAll("\\p{M}", "");
        return SALAS.contains(limpia) || SALA_PRIVADA.matcher(limpia).matches() ? limpia : null;
    }

    private static boolean esPrivada(String sala) {
        return sala != null && sala.startsWith(PREFIJO_PRIVADA);
    }

    private Map<String, String> leerParametros(WebSocketSession session) {
        if (session.getUri() == null) {
            return Map.of();
        }
        Map<String, String> resultado = new HashMap<>();
        UriComponentsBuilder.fromUri(session.getUri()).build(true).getQueryParams()
                .forEach((k, v) -> {
                    if (!v.isEmpty() && v.get(0) != null) {
                        resultado.put(k, URLDecoder.decode(v.get(0), StandardCharsets.UTF_8));
                    }
                });
        return resultado;
    }

    private boolean esJson(WebSocketSession session) {
        return Boolean.TRUE.equals(session.getAttributes().get(ATTR_JSON));
    }

    private String salaDe(WebSocketSession session) {
        return (String) session.getAttributes().get(ATTR_SALA);
    }

    private String usuarioDe(WebSocketSession session) {
        return (String) session.getAttributes().getOrDefault(ATTR_USUARIO, "?");
    }

    private String cidDe(WebSocketSession session) {
        return (String) session.getAttributes().getOrDefault(ATTR_CID, session.getId());
    }
}
