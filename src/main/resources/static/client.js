// client.js
import SockJS from 'sockjs-client';
import Stomp from 'stompjs';

const SOCKET_URL = 'http://localhost:8080/ws'; // Your SockJS endpoint
const socket = new SockJS(SOCKET_URL);
const stompClient = Stomp.over(socket);

// Optional: silence debug logging
stompClient.debug = () => {};

stompClient.connect({}, (frame) => {
  console.log('✅ Connected:', frame);

  // Subscribe to your topic
  stompClient.subscribe('/topic/binance', (message) => {
    console.log('📩 Received:', message.body);
  });

  console.log('📡 Listening for Binance updates...');
}, (error) => {
  console.error('❌ Connection error:', error);
});
