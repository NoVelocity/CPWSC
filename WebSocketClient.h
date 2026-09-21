#ifndef WEBSOCKETCLIENT_H
#define WEBSOCKETCLIENT_H

#include <WiFiClient.h>

class WebSocketClient {
public:

	WebSocketClient(bool secure = false);

	~WebSocketClient();

	bool connect(String host, String path, int port);

	bool isConnected();

	void disconnect();

	void send(const String& str);

	bool getMessage(String& message);

	void setAuthorizationHeader(String header);

private:
	void sendFrame(uint8_t opcode, const uint8_t *payload, size_t size);

	bool readExact(uint8_t *buf, size_t len, unsigned long timeoutMs);

	void write(uint8_t data);

	void write(const char *str);

	String generateKey();

	WiFiClient *client;

	String authorizationHeader = "";

	bool websocketEstablished = false;

};

#endif //WEBSOCKETCLIENT_H
