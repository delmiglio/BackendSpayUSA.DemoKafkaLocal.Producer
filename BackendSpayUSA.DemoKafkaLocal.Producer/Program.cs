
using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Serilog;

namespace BackendSpayUSA.DemoKafkaLocal.Producer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();
            logger.Information("Envio de mensagens com Kafka");


            string bootstrapServers = "localhost:9092";
            string nomeTopic = "principal_topic";

            logger.Information($"BootstrapServers = {bootstrapServers}");
            logger.Information($"Topic = {nomeTopic}");

            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = bootstrapServers
                };

                using (var producer = new ProducerBuilder<Null, string>(config).Build())
                {

                    var result = await producer.ProduceAsync(
                        nomeTopic,
                        new Message<Null, string>
                        { Value = "Mensagem Teste Tópico Principal" });

                    logger.Information(
                        $"Mensagem: {result.Value} | " +
                        $"Status: { result.Status.ToString()}");
                }

                logger.Information("Concluído o envio de mensagens");
            }
            catch (Exception ex)
            {
                logger.Error($"Exceção: {ex.GetType().FullName} | " +
                             $"Mensagem: {ex.Message}");
            }
        }
    }
}
