import {Kafka} from 'kafkajs';

const kafka = new Kafka({
    clientId: "analytic-service",
    brokers: ["localhost:9094", "localhost:9095", "localhost:9096"]
});

const consumer = kafka.consumer({
    groupId: "analytic-service"
});

const run = async () => {
    try {
        await consumer.connect();
        await consumer.subscribe({
            topics: ["payment-successful", "order-successful", "email-successful"],
            fromBeginning: true
        });

        await consumer.run({
            eachMessage: async ({topic, partition, message}) => {

                switch (topic) {
                    case "order-successful": {
                        const value = message.value.toString();
                        const {userId, orderId} = JSON.parse(value);

                        console.log(`Analytic consumer: Order Id ${orderId} created for user ${userId}`);
                    }
                        break;
                    case "email-successful": {
                        const value = message.value.toString();
                        const {userId, emailId} = JSON.parse(value);

                        console.log(`Analytic consumer: Email Id ${emailId} sent to user id ${userId}`);
                    }
                        break;
                    case "payment-successful": {
                        const value = message.value.toString();
                        const {userId, cart} = JSON.parse(value);

                        const total = cart.reduce((acc, item) => acc + item.price, 0).toFixed(2);

                        console.log(`Analytic consumer: User ${userId} made a payment of $${total}`);
                    }
                        break;
                    default:
                        console.log(`Unknown topic: ${topic}`);
                        return;
                }
            }
        });
    } catch (err) {
        console.log(err);
    }
};