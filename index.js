require("dotenv").config();
const mqtt = require("mqtt");
const mysql = require("mysql2/promise");
const amqplib = require('amqplib');

const queue = process.env.RABBITMQ_QUEUE;

// Crear conexión a la base de datos
const connectionn = mysql.createPool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
});

let mqttClient;
let rabbitConnection;
let rabbitChannel;
let mqttConnected = false;

// Función para conectar a MQTT con manejo de reconexión
const connectMQTT = () => {
    console.log("Attempting to connect to MQTT...");
    mqttClient = mqtt.connect(process.env.MQTT_URL, {
        username: process.env.MQTT_USERNAME,
        password: process.env.MQTT_PASSWORD
    });

    mqttClient.on("connect", () => {
        mqttConnected = true;
        console.log("Connected to MQTT");
        mqttClient.subscribe(process.env.MQTT_TOPIC, (err) => {
            if (!err) {
                console.log(`Subscribed to ${process.env.MQTT_TOPIC}`);
            } else {
                console.error(`Failed to subscribe to ${process.env.MQTT_TOPIC}:`, err);
            }
        });
    });

    mqttClient.on("error", (err) => {
        mqttConnected = false;
        console.error("Connection to MQTT failed:", err);
        setTimeout(connectMQTT, 5000); // Intentar reconectar después de 5 segundos
    });

    mqttClient.on("close", () => {
        mqttConnected = false;
        console.log("MQTT connection closed. Attempting to reconnect...");
        setTimeout(connectMQTT, 5000); // Intentar reconectar después de 5 segundos
    });
};

// Función para conectar a RabbitMQ con manejo de reconexión
const connectRabbitMQ = async () => {
    try {
        console.log("Connecting to RabbitMQ...");
        rabbitConnection = await amqplib.connect(process.env.RABBITMQ_URL, {
            username: process.env.RABBITMQ_USERNAME,
            password: process.env.RABBITMQ_PASSWORD,
        });
        console.log("Connected to RabbitMQ");
        rabbitChannel = await rabbitConnection.createChannel();
        await rabbitChannel.assertQueue(queue, { durable: true });

        rabbitConnection.on("error", (err) => {
            console.error("RabbitMQ connection error:", err);
            setTimeout(connectRabbitMQ, 5000); // Intentar reconectar después de 5 segundos
        });

        rabbitConnection.on("close", () => {
            console.log("RabbitMQ connection closed. Attempting to reconnect...");
            setTimeout(connectRabbitMQ, 5000); // Intentar reconectar después de 5 segundos
        });

        // Manejar mensajes MQTT
        mqttClient.on("message", async (topic, message) => {
            try {
                console.log("Message received:", message.toString());
                const data = JSON.parse(message.toString());
                await saveData(data);
                await sendToQueue(data);
            } catch (error) {
                console.error("Error processing message:", error);
            }
        });

    } catch (error) {
        console.error("Error connecting to RabbitMQ:", error);
        setTimeout(connectRabbitMQ, 5000); // Intentar reconectar después de 5 segundos
    }
};

// Guardar datos en la base de datos
const saveData = async (data) => {
    try {
        const [result] = await connectionn.execute(
            "INSERT INTO sensorData(sensorId, valor, isDeleted, updatedAt, createdBy, updatedBy, nameSensor, codeOfProduct) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [
                data.sensorId, data.valor, false, new Date(), 1, 1, data.nameSensor, data.codeOfProduct
            ]);
        
     if(data.nameSensor=="modulo de sensor de luz fotoresistencia ldr" && data.valor<270){
            const [saveanomaly] = await connectionn.execute(
                "INSERT INTO anomaly(sensorDataId,createdBy,updatedBy) VALUES(?,?,?)", [result.insertId, 1, 1]
            )

        }else if (data.nameSensor=="sensor de voltaje Ac Zmpt101b"&& data.valor<14.4){
            const [saveanomaly] = await connectionn.execute(
                "INSERT INTO anomaly(sensorDataId,createdBy,updatedBy) VALUES(?,?,?)", [result.insertId, 1, 1]
            )
        }else if (data.nameSensor=="modulo de sensor de corriente Acs712" && data.valor<6.4){
            const [saveanomaly] = await connectionn.execute(
                "INSERT INTO anomaly(sensorDataId,createdBy,updatedBy) VALUES(?,?,?)", [result.insertId, 1, 1]
            )
        }
        
        console.log("Data saved:", result);
    } catch (error) {
        console.error("Error saving data:", error);
    }
};

// Enviar datos a RabbitMQ
const sendToQueue = async (data) => {
    data={
        ...data,
        anomalie: false,
        createdAt: new Date(),
    }
    try {
       
            

     if (data.nameSensor=="sensor de voltaje Ac Zmpt101b"&& data.valor<14.4){
            data.anomalie=true;
           
    }else if (data.nameSensor=="modulo de sensor de corriente Acs712" && data.valor<6.4){
            data.anomalie=true;
            
     }else if (data.nameSensor=="modulo de sensor de luz fotoresistencia ldr" && data.valor<220){
        data.anomalie=true;

     }
        

        rabbitChannel.sendToQueue(queue, Buffer.from(JSON.stringify(data)));
        console.log("Data sent to queue:", data);
    } catch (error) {
        console.error("Error sending to queue:", error);
    }
};

// Iniciar conexiones
connectMQTT();
connectRabbitMQ();
