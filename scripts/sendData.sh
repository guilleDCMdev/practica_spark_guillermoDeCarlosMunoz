#!/bin/bash

# Function to check if port is open
is_port_open() {
    nc -z localhost 9999
}

# Function to send data to connected clients
send_data() {
    while true; do
        if is_port_open; then
            echo "Sending data..."
            echo "Hello, client!" | nc localhost 9999
        else
            echo "Port 9999 is not open. Starting server..."
        fi
        sleep 5
    done
}

# Start sending data
send_data