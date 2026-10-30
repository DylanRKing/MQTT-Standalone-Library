using Mqtt.Internal.Codec;

namespace Mqtt.Tests.Codec;

public class TopicValidatorTests
{
    [Theory]
    [InlineData("a", true)]
    [InlineData("a/b/c", true)]
    [InlineData("/", true)]
    [InlineData("a//b", true)]
    [InlineData("$aws/things/x/shadow/update", true)]
    [InlineData("", false)]
    [InlineData("a/+", false)]
    [InlineData("a/#", false)]
    [InlineData("a+b", false)]
    public void TopicName(string topic, bool valid)
    {
        Assert.Equal(valid, new TopicValidator().IsValidTopicName(topic));
    }

    [Theory]
    [InlineData("a/b", true)]
    [InlineData("#", true)]
    [InlineData("+", true)]
    [InlineData("a/#", true)]
    [InlineData("a/+/b", true)]
    [InlineData("+/+", true)]
    [InlineData("/#", true)]
    [InlineData("+/#", true)]
    [InlineData("", false)]
    [InlineData("a#", false)]
    [InlineData("a/#/b", false)]
    [InlineData("##", false)]
    [InlineData("a+", false)]
    [InlineData("+a", false)]
    [InlineData("a/b+/c", false)]
    public void TopicFilter(string filter, bool valid)
    {
        Assert.Equal(valid, new TopicValidator().IsValidTopicFilter(filter));
    }
}
