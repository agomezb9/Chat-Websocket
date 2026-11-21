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
 *
 * Salas privadas: cualquier nombre "priv-" + 3 a 32 letras/números se crea al entrar el primero,
 * no aparece en /salas y se borra cuando queda vacía.
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

    private static final String SALA_POR_DEFECTO = "general";
    private static final String PREFIJO_PRIVADA = "priv-";
    private static final Pattern SALA_PRIVADA = Pattern.compile("priv-[a-z0-9]{3,32}");

    private static final String ATTR_SALA = "sala";
    private static final String ATTR_USUARIO = "usuario";
    private static final String ATTR_JSON = "json";

    private static final DateTimeFormatter HORA =
            DateTimeFormatter.ofPattern("HH:mm").withZone(ZoneId.systemDefault());

    private static final ObjectMapper JSON = new ObjectMapper();

    /**
     * Lo que se envía a los clientes web (formato=json). Los clientes de texto plano reciben
     * la misma información convertida a una línea de texto.
     * tipo: texto | audio | sistema | pong.   evento (solo en "sistema"): bienvenida | sala.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Msg(String tipo, String evento, String id, String usuario,
                      String sala, Long ts, String contenido) {
    }

    /** sala -> sesiones conectadas en esa sala */
    private final Map<String, Set<WebSocketSession>> salas = new ConcurrentHashMap<>();

    /** sala privada -> clave ("" si no tiene) */
    private final Map<String, String> clavesSalas = new ConcurrentHashMap<>();

    /** id de sesión -> sesión "segura" para enviar desde varios hilos a la vez */
    private final Map<String, WebSocketSession> sesiones = new ConcurrentHashMap<>();

    /** Protege crear/borrar salas privadas y entrar/salir de ellas. */
    private final Object lock = new Object();

    public ChatSalasHandler() {
        for (String sala : SALAS) {
            salas.put(sala, ConcurrentHashMap.newKeySet());
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
        session.getAttributes().put(ATTR_USUARIO, usuario);
        session.getAttributes().put(ATTR_JSON, "json".equalsIgnoreCase(params.get("formato")));

        String sala = normalizarSala(params.get("sala"));
        if (sala == null) {
            sala = SALA_POR_DEFECTO;
        }

        System.out.println("🔌 [CHAT] Conectado: " + usuario + " (" + session.getId() + ")");
        entregar(segura, new Msg("sistema", "bienvenida", session.getId(), usuario, null, ahora(),
                "Bienvenido/a " + usuario + ". Salas públicas: " + String.join(", ", SALAS)
                        + ". Escribe /ayuda para ver los comandos."));

        if (!agregar(segura, sala, params.getOrDefault("clave", ""))) {
            aviso(segura, "🔒 Clave incorrecta para esa sala privada.");
            session.close(CloseStatus.POLICY_VIOLATION.withReason("Clave incorrecta"));
            return;
        }
        session.getAttributes().put(ATTR_SALA, sala);
        anunciarEntrada(session, segura, sala);
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
        difundir(sala, mensaje("texto", session, sala, texto));
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
        difundir(sala, mensaje("audio", session, sala, datos));
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
        } else if (!agregar(segura, nueva, clave)) {
            // Si la clave falla, el usuario sigue en la sala donde estaba
            aviso(segura, "🔒 Clave incorrecta para esa sala privada.");
        } else {
            session.getAttributes().put(ATTR_SALA, nueva);
            if (actual != null) {
                quitar(segura, actual);
                difundir(actual, avisoMsg("🔴 " + usuarioDe(session) + " salió de la sala " + actual.toUpperCase()));
            }
            anunciarEntrada(session, segura, nueva);
        }
    }

    // ------------------------------------------------------------------
    // Manejo de salas
    // ------------------------------------------------------------------

    /**
     * Mete la sesión en la sala. En una sala privada que no existe la crea y guarda la clave;
     * si ya existe y tiene clave, solo deja entrar si coincide. Devuelve false si se rechaza.
     */
    private boolean agregar(WebSocketSession segura, String sala, String clave) {
        synchronized (lock) {
            Set<WebSocketSession> grupo = salas.get(sala);
            if (esPrivada(sala)) {
                if (grupo == null) {
                    grupo = ConcurrentHashMap.newKeySet();
                    salas.put(sala, grupo);
                    clavesSalas.put(sala, clave);
                } else if (!claveCorrecta(clavesSalas.getOrDefault(sala, ""), clave)) {
                    return false;
                }
            }
            grupo.add(segura);
            return true;
        }
    }

    /** Saca la sesión de la sala; una sala privada que queda vacía se elimina. */
    private void quitar(WebSocketSession segura, String sala) {
        synchronized (lock) {
            Set<WebSocketSession> grupo = salas.get(sala);
            if (grupo == null) {
                return;
            }
            grupo.remove(segura);
            if (esPrivada(sala) && grupo.isEmpty()) {
                salas.remove(sala);
                clavesSalas.remove(sala);
            }
        }
    }

    private void anunciarEntrada(WebSocketSession session, WebSocketSession segura, String sala) {
        entregar(segura, new Msg("sistema", "sala", null, null, sala, ahora(),
                "✅ Entraste a la sala " + sala.toUpperCase()));
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
        Set<WebSocketSession> grupo = sala == null ? null : salas.get(sala);
        return grupo == null ? Set.of() : grupo;
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
        if (destino == null || !destino.isOpen()) {
            return;
        }
        try {
            String salida = esJson(destino) ? JSON.writeValueAsString(msg) : comoTexto(msg);
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
        return new Msg(tipo, null, session.getId(), usuarioDe(session), sala, ahora(), contenido);
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
}
