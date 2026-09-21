//#define DEBUG

#include "WebSocketClient.h"
#include <WiFiClientSecure.h>

#define WS_FIN            0x80
#define WS_OPCODE_MASK    0x0F
#define WS_OPCODE_TEXT    0x01
#define WS_OPCODE_BINARY  0x02
#define WS_OPCODE_CLOSE   0x08
#define WS_OPCODE_PING    0x09
#define WS_OPCODE_PONG    0x0A

#define WS_MASK           0x80
#define WS_SIZE16         126
#define WS_SIZE64         127

#define WS_MAX_PAYLOAD    8192   // frames larger than this drop the connection (RAM protection)
#define WS_READ_TIMEOUT   2000   // ms to wait for the rest of a frame that already started

#ifdef DEBUG
#define DEBUG_WS Serial.println
#else
#define DEBUG_WS(MSG)
#endif

WebSocketClient::WebSocketClient(bool secure) {
	if (secure)
		this->client = new WiFiClientSecure;
	else
		this->client = new WiFiClient;
}

WebSocketClient::~WebSocketClient() {
	delete this->client;
}

void WebSocketClient::setAuthorizationHeader(String header) {
	this->authorizationHeader = header;
}

String WebSocketClient::generateKey() {
	String key = "";
	for (int i = 0; i < 22; ++i) {
		int r = random(0, 3);
		if (r == 0)
			key += (char) random(48, 57);
		else if (r == 1)
			key += (char) random(65, 90);
		else if (r == 2)
			key += (char) random(97, 122);
	}
	return key;
}
void WebSocketClient::write(uint8_t data) {
    if (client->connected())
        client->write(data);
}

void WebSocketClient::write(const char *data) {
    if (client->connected())
        client->write(data);
}

bool WebSocketClient::connect(String host, String path, int port) {
    if (!client->connect(host.c_str(), port))
        return false;

	// send handshake
	String handshake = "GET " + path + " HTTP/1.1\r\n"
			"Host: " + host + "\r\n"
			"Connection: Upgrade\r\n"
			"Upgrade: websocket\r\n"
			"Sec-WebSocket-Version: 13\r\n"
			"Sec-WebSocket-Key: " + generateKey() + "==\r\n";

	if (authorizationHeader != "")
		handshake += "Authorization: " + authorizationHeader + "\r\n";

	handshake += "\r\n";

	DEBUG_WS("[WS] sending handshake");
	DEBUG_WS(handshake);

    write(handshake.c_str());

	// success criteria
	bool hasCorrectStatus = false;
	bool isUpgrade = false;
	bool isWebsocket = false;
	bool hasAcceptedKey = false;

	bool endOfResponse = false;

	// handle response headers
	String s;
	while (!endOfResponse && (s = client->readStringUntil('\n')).length() > 0) {
		DEBUG_WS("[WS][RX] " + s);
		// HTTP Status
		if (s.indexOf("HTTP/") != -1) {
			auto status = s.substring(9, 12);
			if (status == "101")
				hasCorrectStatus = true;
			else {
				DEBUG_WS("[WS] wrong status: " + status);
				return false;
			}
		}
		// Headers
		else if (s.indexOf(":") != -1) {
			auto col = s.indexOf(":");
			auto key = s.substring(0, col);
			auto value = s.substring(col + 2, s.length() - 1);

			if (key == "Connection" && (value == "Upgrade" || value == "upgrade"))
				isUpgrade = true;

			else if (key == "Sec-WebSocket-Accept")
				hasAcceptedKey = true;

			else if (key == "Upgrade" && value == "websocket")
				isWebsocket = true;
		}

		else if (s == "\r")
			endOfResponse = true;
	}

	bool success = hasCorrectStatus && isUpgrade && isWebsocket && hasAcceptedKey;

	if (success) {
		DEBUG_WS("[WS] sucessfully connected");
        this->websocketEstablished = true;
    }
	else {
		DEBUG_WS("[WS] could not connect");
        this->disconnect();
	}

	return success;
}

bool WebSocketClient::isConnected() {
	return this->websocketEstablished && client->connected();
}

void WebSocketClient::disconnect() {
	client->stop();
    this->websocketEstablished = false;
}

// Client-to-server frames must be masked (RFC 6455 5.3). The whole frame is
// assembled in one buffer and sent with a single write.
void WebSocketClient::sendFrame(uint8_t opcode, const uint8_t *payload, size_t size) {
	if (!client->connected()) {
		DEBUG_WS("[WS] not connected...");
		return;
	}
	if (size > 0xFFFF) {
		DEBUG_WS("[WS] payload too large");
		return;
	}

	size_t headerLen = (size > 125) ? 8 : 6;   // 2 (+2 ext length) + 4 mask
	uint8_t *buf = (uint8_t *) malloc(headerLen + size);
	if (!buf) return;

	size_t p = 0;
	buf[p++] = WS_FIN | opcode;
	if (size > 125) {
		buf[p++] = WS_MASK | WS_SIZE16;
		buf[p++] = (uint8_t) (size >> 8);
		buf[p++] = (uint8_t) (size & 0xFF);
	} else {
		buf[p++] = WS_MASK | (uint8_t) size;
	}

	uint8_t mask[4];
	for (int i = 0; i < 4; ++i) {
		mask[i] = random(0, 256);
		buf[p++] = mask[i];
	}

	for (size_t i = 0; i < size; ++i) {
		buf[p + i] = payload[i] ^ mask[i % 4];
	}

	client->write(buf, p + size);
	free(buf);
}

void WebSocketClient::send(const String& str) {
	DEBUG_WS("[WS] sending: " + str);
	sendFrame(WS_OPCODE_TEXT, (const uint8_t *) str.c_str(), str.length());
}

bool WebSocketClient::readExact(uint8_t *buf, size_t len, unsigned long timeoutMs) {
	unsigned long start = millis();
	size_t got = 0;
	while (got < len) {
		int avail = client->available();
		if (avail > 0) {
			size_t want = len - got;
			size_t n = ((size_t) avail < want) ? (size_t) avail : want;
			int r = client->read(buf + got, n);
			if (r > 0) got += r;
		} else {
			if (!client->connected() || millis() - start > timeoutMs) return false;
			delay(1);
		}
	}
	return true;
}

bool WebSocketClient::getMessage(String& message) {
	if (!client->connected()) { return false; }
	if (client->available() < 2) { return false; }

	// Once a frame has started we must consume all of it, otherwise the stream
	// is desynchronized. Any failure from here on drops the connection.
	uint8_t hdr[2];
	if (!readExact(hdr, 2, WS_READ_TIMEOUT)) { disconnect(); return false; }

	uint8_t opcode = hdr[0] & WS_OPCODE_MASK;
	bool hasMask = (hdr[1] & WS_MASK) != 0;
	uint32_t length = hdr[1] & ~WS_MASK;

	if (length == WS_SIZE16) {
		uint8_t ext[2];
		if (!readExact(ext, 2, WS_READ_TIMEOUT)) { disconnect(); return false; }
		length = ((uint32_t) ext[0] << 8) | ext[1];
	} else if (length == WS_SIZE64) {
		uint8_t ext[8];
		if (!readExact(ext, 8, WS_READ_TIMEOUT)) { disconnect(); return false; }
		if (ext[0] || ext[1] || ext[2] || ext[3]) { disconnect(); return false; }
		length = ((uint32_t) ext[4] << 24) | ((uint32_t) ext[5] << 16) |
				 ((uint32_t) ext[6] << 8) | ext[7];
	}

	if (length > WS_MAX_PAYLOAD) {
		DEBUG_WS("[WS] frame too large, dropping connection");
		disconnect();
		return false;
	}

	uint8_t mask[4] = {0};
	if (hasMask && !readExact(mask, 4, WS_READ_TIMEOUT)) { disconnect(); return false; }

	// ---- control frames (ping / pong / close) ----
	if (opcode & 0x08) {
		if (length > 125) { disconnect(); return false; }   // invalid per RFC 6455

		uint8_t payload[125];
		if (!readExact(payload, length, WS_READ_TIMEOUT)) { disconnect(); return false; }
		if (hasMask) {
			for (uint32_t i = 0; i < length; ++i) payload[i] ^= mask[i % 4];
		}

		if (opcode == WS_OPCODE_PING) {
			DEBUG_WS("[WS] ping -> pong");
			sendFrame(WS_OPCODE_PONG, payload, length);   // echo payload back
		} else if (opcode == WS_OPCODE_CLOSE) {
			DEBUG_WS("[WS] close frame received");
			sendFrame(WS_OPCODE_CLOSE, payload, length >= 2 ? 2 : 0);   // echo status code
			disconnect();
		}
		// pong: nothing to do

		return false;   // control frames are never reported as messages
	}

	// ---- data frames (text / binary) ----
	message = "";
	message.reserve(length);

	uint8_t chunk[64];
	uint32_t done = 0;
	while (done < length) {
		size_t n = (length - done < sizeof(chunk)) ? (length - done) : sizeof(chunk);
		if (!readExact(chunk, n, WS_READ_TIMEOUT)) { disconnect(); return false; }
		for (size_t i = 0; i < n; ++i) {
			message += (char) (hasMask ? (chunk[i] ^ mask[(done + i) % 4]) : chunk[i]);
		}
		done += n;
	}

	return true;
}
