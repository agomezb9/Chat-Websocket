package co.vinni.dmos.config;

import co.vinni.dmos.server.ChatSalasHandler;
import co.vinni.dmos.server.NetworkSocketHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

@Configuration
@EnableWebSocket // Habilita la infraestructura de WebSockets de Spring
public class WebSocketConfig implements WebSocketConfigurer {

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(new NetworkSocketHandler(), "/network-monitor")
                .setAllowedOrigins("*"); // Permite conexiones desde cualquier origen/cliente

        // Chat con salas: general, musica, deportes
        registry.addHandler(new ChatSalasHandler(), "/chat")
                .setAllowedOrigins("*");
    }

    /**
     * Por defecto el servidor corta la conexión si un mensaje pesa más de 64 KB, y un audio de
     * 8-10 segundos en base64 pesa entre 120 KB y 300 KB. Se sube el límite a 2 MB.
     */
    @Bean
    public ServletServerContainerFactoryBean createWebSocketContainer() {
        ServletServerContainerFactoryBean container = new ServletServerContainerFactoryBean();
        container.setMaxTextMessageBufferSize(ChatSalasHandler.LIMITE_BUFFER);
        container.setMaxBinaryMessageBufferSize(ChatSalasHandler.LIMITE_BUFFER);
        return container;
    }
}