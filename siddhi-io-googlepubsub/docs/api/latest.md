# API Docs - v1.0.0-SNAPSHOT

## Sink

### googlepubsub *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#sink">(Sink)</a>*

<p style="word-wrap: break-word">The Google Pub Sub sink publishes the events into a Google Pub Sub processed by WSO2 Stream Processor. If a topic doesn't exist in the server, Google Pub Sub sink creates a topic.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@sink(type="googlepubsub", projectid="<STRING>", topicid="<STRING>", idletimeout="<INT>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">projectid</td>
        <td style="vertical-align: top; word-wrap: break-word">The unique ID of the project within which the topic is created.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">topicid</td>
        <td style="vertical-align: top; word-wrap: break-word">The topic id to which events should be published. Only one topic must be specified.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
    <tr>
        <td style="vertical-align: top">idletimeout</td>
        <td style="vertical-align: top; word-wrap: break-word">Idle timeout of the connection.</td>
        <td style="vertical-align: top">50</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">Yes</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@sink(type = 'googlepubsub', @map(type= 'text'), projectid = 'sp-path-1547649404768', topicid ='topicA', )
define stream inputStream (message string);
```
<p style="word-wrap: break-word">This example shows how to publish messages to a topic in the Google Pub Sub server with all supportive configurations.Accordingly, the messages are published to a topic named topicA in the project having a project id  'sp-path-1547649404768'.If the required topic already exists in the particular project the messages are directly published to that topic.If the required topic does not exist a new topic is created to the required topic id and then the messages are published to the particular topic.</p>

## Source

### googlepubsub *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source">(Source)</a>*

<p style="word-wrap: break-word">A Google Pub Sub receives events to be processed by WSO2 SP from a topic.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@source(type="googlepubsub", projectid="<STRING>", topicid="<STRING>", subscriptionid="<STRING>", idletimeout="<INT>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">projectid</td>
        <td style="vertical-align: top; word-wrap: break-word">The unique ID of the project within which the topic is created. </td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">topicid</td>
        <td style="vertical-align: top; word-wrap: break-word">The unique id of the topic from which the events are received.Only one topic must be specified.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">subscriptionid</td>
        <td style="vertical-align: top; word-wrap: break-word">The unique id of the subscription from which messages should be retrieved.This subscription id connects the topic to a subscriber application that receives and processes messages published to the topic. A topic can have multiple subscriptions,but a given subscription belongs to a single topic.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">idletimeout</td>
        <td style="vertical-align: top; word-wrap: break-word">Idle timeout of the connection.</td>
        <td style="vertical-align: top">50</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@source(type='googlepubsub',@map(type='text'),topicId='topicA',projectId='sp-path-1547649404768',subscriptionId='subB',)
define stream Test(name String);
```
<p style="word-wrap: break-word">This example shows how to subscribe to a googlepubsub topic with all supporting configurations.With the following configuration the identified source,will subscribe to a topic named as topicA which resides in a googlepubsub instance withthe project id of 'my-project-test-227508'.This Google Pub Sub source configuration listens to the googlepubsub topicA.The events are received in the text format and mapped to a Siddhi event,and sent to a the Test stream.</p>

