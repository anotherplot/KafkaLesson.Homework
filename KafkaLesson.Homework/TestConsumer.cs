using Dodo.Kafka.Consumer;

namespace KafkaLesson.Homework;

public class TestConsumer : IConsumer<TestEvent>
{
    public async Task Consume(TestEvent ev, CancellationToken ct)
    {
        Console.WriteLine($"Received event {ev.Id} with version {ev.Version}");
        EventStorage.Instance.AddConsumedEvent(ev);

        var processedEvents = EventStorage.Instance.GetProcessedEvent();
        var processedEvent = processedEvents.LastOrDefault(e => e.Id == ev.Id);

        if (processedEvent == null || ev.Version > processedEvent.Version)
        {
            Console.WriteLine($"Processed event {ev.Id} with version {ev.Version}.");
            EventStorage.Instance.AddProcessedEvent(ev);
        }
        else
        {
            Console.WriteLine(
                $"Ignored event {ev.Id} with version {ev.Version}. Highest version is {processedEvent.Version}");
        }

        await Task.CompletedTask;
    }
}
