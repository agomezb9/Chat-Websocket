# WebSock_SB: Chat por salas con WebSocket

Servidor de chat hecho con **Spring Boot** y **WebSocket**. Tiene tres salas públicas (`general`, `musica`, `deportes`), salas privadas por link, emojis y mensajes de voz. Se puede usar desde el navegador (`chat.html`) o desde un cliente de consola en **Java**.

## Requisitos

- Java 21
- Maven (o un IDE como IntelliJ que lo traiga)

## Cómo ejecutarlo

1. Ejecuta `NetworkApplication` (servidor). Queda escuchando en el puerto **8862** (se cambia en `application.yml` o con la variable de entorno `PORT`).
2. Abre `http://localhost:8862/chat.html` en el navegador, o ejecuta `ChatCliente` (consola). Puedes abrir varios a la vez, uno por usuario. En IntelliJ activa *Allow multiple instances* para abrir varios `ChatCliente`.
3. Escribe en una sala; solo lo verán quienes estén en esa misma sala.

`ChatCliente` también se puede lanzar con argumentos para saltarse las preguntas:

```
ChatCliente <nombre> <sala> [url-del-servidor]
ChatCliente Ana musica
ChatCliente Ana musica ws://192.168.1.10:8862/chat
```

## Cliente web (`chat.html`)

- **Pantalla inicial:** dos botones, *Entrar al chat público* (sala General) y *Crear sala privada* (con clave opcional).
- **Sala privada:** al crearla, el botón *Copiar link de invitación* copia un link como `https://…/chat.html?sala=priv-k3j9x0a8bd`. Quien lo abre solo ve un cuadro para escribir su nombre (Enter entra como invitado) y, si la sala tiene clave, otro para la clave. La clave nunca va dentro del link: hay que pasarla aparte.
- **Emojis:** el botón 😊 abre un panel de emojis frecuentes.
- **Mensajes de voz:** el botón 🎤 graba (hasta 60 s); al pulsarlo de nuevo se envía. El navegador pide permiso de micrófono, y solo lo permite si la página abre por `https://` o `localhost`.
- **Conexión segura:** usa `wss://` automáticamente cuando la página se abre por `https://`, y `ws://` en `localhost`.

## Publicar con un link para que se conecte cualquiera

El chat y la página salen del mismo servidor, así que basta con publicar el servidor.

**Opción A: desde tu PC (pruebas, demos, clase).** Con el servidor corriendo en el puerto 8862, en otra terminal:

```
cloudflared tunnel --url http://localhost:8862
# o con ngrok:
ngrok http 8862
```

Te entrega una URL pública `https://…/chat.html` para enviar por WhatsApp o Discord. Solo funciona mientras tu PC y el servidor estén encendidos.

**Opción B: en la nube 24/7 (Render, Railway, Koyeb).** Sube el repositorio a GitHub y crea un servicio a partir del `Dockerfile` de la raíz. La plataforma asigna el puerto en la variable `PORT`, que ya lee `application.yml`.

## Estructura del proyecto

```
Dockerfile                             Imagen para la nube (compila con Maven, corre con Java 21)
src/main/resources/
├── application.yml                    Puerto (PORT o 8862)
└── static/chat.html                   Cliente web
src/main/java/co/vinni/dmos/
├── server/
│   ├── NetworkApplication.java        Arranca Spring Boot
│   ├── ChatSalasHandler.java          Lógica del chat por salas   (ws://host:8862/chat)
│   └── NetworkSocketHandler.java      Monitor de red original     (ws://host:8862/network-monitor)
├── config/
│   └── WebSocketConfig.java           Registra los endpoints y sube el límite de mensaje a 2 MB
└── cliente/
    └── ChatCliente.java               Cliente de consola
```

## Cómo funciona

### Servidor

`WebSocketConfig` asocia cada ruta con su manejador: `/chat` con `ChatSalasHandler` y `/network-monitor` con `NetworkSocketHandler`. El chat funciona así:

1. **Conexión.** El cliente se conecta a `ws://localhost:8862/chat?usuario=Ana&sala=musica`. Todos los parámetros son opcionales: sin nombre se asigna `Invitado-xxxx`, sin sala (o con una inválida) se entra a `general`. Otros dos parámetros: `clave` (para salas privadas) y `formato=json` (lo usa el cliente web).
2. **Salas.** El servidor guarda en memoria un mapa `sala → conjunto de sesiones`. Cada usuario está en una sola sala a la vez.
3. **Salas privadas.** Cualquier nombre `priv-` + 3 a 32 letras o números se crea al entrar el primero. No aparece en `/salas` y se borra cuando queda vacía. Si el primero puso clave, los demás deben enviarla; con una clave incorrecta el servidor cierra la conexión con el código 1008.
4. **Mensajes.** Un mensaje se reenvía solo a las sesiones de la sala del remitente. Los textos que empiezan con `/` son comandos.
5. **Avisos.** Cuando alguien entra, sale o cambia de sala, se avisa a los demás de esa sala.
6. **Desconexión.** Al cerrar la conexión se saca al usuario de su sala (y se borra la sala si era privada y quedó vacía).

Para que varios usuarios puedan escribir al mismo tiempo sin que falle el envío, cada sesión se envuelve en un `ConcurrentWebSocketSessionDecorator`.

### Formato de los mensajes

Lo que **envía** el cliente web, por el mismo WebSocket:

```json
{ "tipo": "texto", "contenido": "Hola! 😂🔥" }
{ "tipo": "audio", "contenido": "data:audio/webm;base64,GkXfo59ChoEB..." }
{ "tipo": "ping" }
```

Lo que **recibe** (con `formato=json`) es un objeto con `tipo` (`texto`, `audio`, `sistema` o `pong`), y según el caso `usuario`, `id` (quién lo envió), `sala`, `ts` (hora en milisegundos) y `contenido`. Los avisos del servidor traen además `evento` (`bienvenida` o `sala`).

Los clientes sin `formato=json`, como `ChatCliente`, siguen enviando y recibiendo texto plano; un audio les llega como una línea que dice que hay un mensaje de voz.

Por defecto el servidor corta la conexión si un mensaje pasa de 64 KB, y un audio de 8 a 10 segundos en base64 pesa entre 120 KB y 300 KB. Por eso `WebSocketConfig` sube el límite a 2 MB, y el servidor rechaza audios de más de 1,5 millones de caracteres.

### Cliente de consola

`ChatCliente` usa el WebSocket que trae Java (`java.net.http`), así que no necesita dependencias extra. Tiene dos partes:

- **El hilo principal** lee lo que escribes en la consola y lo envía al servidor.
- **Un oyente** recibe los mensajes del servidor y los imprime en la consola.

Para cambiar de sala se puede escribir solo `1`, `2` o `3`, que el cliente convierte en `/unirse general`, `/unirse musica` o `/unirse deportes`.

## Comandos

| Comando | Qué hace |
|---|---|
| `1` / `2` / `3` | Cambia a General / Música / Deportes (atajo del cliente de consola) |
| `/salas` | Muestra las salas públicas y cuántos usuarios hay en cada una |
| `/unirse <sala> [clave]` | Cambia de sala (acepta `Música`, `MUSICA`, `priv-k3j9x0a8bd`, etc.) |
| `/usuarios` | Lista los usuarios de tu sala |
| `/nombre <nuevo>` | Cambia tu nombre |
| `/ayuda` | Muestra la ayuda del servidor |
| `/menu` | Muestra el menú del cliente de consola |
| `/salir` | Se desconecta (cliente de consola) |
| `PING` | El servidor responde `PONG` |
| *cualquier otro texto* | Se envía como mensaje a tu sala |

## Limitaciones

- Todo vive en memoria: si se reinicia el servidor se pierden los usuarios conectados y las salas privadas. No se guarda historial de mensajes.
- No hay autenticación. Cualquiera puede usar cualquier nombre y los nombres pueden repetirse.
- Una sala privada solo está protegida por lo difícil que es adivinar su código y por su clave opcional. Quien tenga el link puede entrar.
- Las salas públicas están fijas en el código (`ChatSalasHandler.SALAS`). Para agregar una basta con añadir su nombre a esa lista.
