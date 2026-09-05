using MonitoringService from './monitoring-service';

/**
 * Fiori elements annotations for the Kafka message monitoring dashboard.
 */

annotate MonitoringService.Messages with @(
    UI: {
        HeaderInfo               : {
            $Type         : 'UI.HeaderInfoType',
            TypeName      : 'Kafka Message',
            TypeNamePlural: 'Kafka Messages',
            Title         : {Value: serviceName},
            Description   : {Value: messageId}
        },
        PresentationVariant      : {
            $Type     : 'UI.PresentationVariantType',
            SortOrder : [{
                $Type     : 'Common.SortOrderType',
                Property  : messageTimestamp,
                Descending: true
            }],
            Visualizations: ['@UI.LineItem']
        },
        SelectionFields          : [
            serviceName,
            correlationId,
            conversationId,
            tenantId
        ],
        LineItem                 : [
            {
                $Type: 'UI.DataField',
                Value: messageTimestamp,
                ![@UI.Importance]: #High
            },
            {
                $Type: 'UI.DataField',
                Value: serviceName,
                ![@UI.Importance]: #High
            },
            {
                $Type: 'UI.DataField',
                Value: correlationId,
                ![@UI.Importance]: #High
            },
            {
                $Type: 'UI.DataField',
                Value: conversationId,
                ![@UI.Importance]: #High
            },
            {
                $Type: 'UI.DataField',
                Value: tenantId,
                ![@UI.Importance]: #High
            }
        ],
        FieldGroup #Identifiers  : {
            $Type: 'UI.FieldGroupType',
            Data : [
                {
                    $Type: 'UI.DataField',
                    Value: correlationId
                },
                {
                    $Type: 'UI.DataField',
                    Value: conversationId
                },
                {
                    $Type: 'UI.DataField',
                    Value: tenantId
                }
            ]
        },
        Facets                   : [],
        HeaderFacets             : [{
            $Type : 'UI.ReferenceFacet',
            ID    : 'IdentifiersFacet',
            Label : 'Identifiers',
            Target: '@UI.FieldGroup#Identifiers'
        }]
    }
);

annotate MonitoringService.Messages with {
    ID               @UI.Hidden;
    serviceName      @title: 'Service Name';
    messageHash      @title: 'Message Hash';
    payloadHash      @title: 'Payload Hash';
    correlationId    @title: 'Correlation ID';
    messageId        @title: 'Message ID';
    conversationId   @title: 'Conversation ID';
    sourceId         @title: 'Source ID';
    eventType        @title: 'Event Type';
    messageType      @title: 'Message Type';
    topic            @title: 'Topic';
    messageTimestamp @title: 'Message Time';
    kafkaPartition   @title: 'Partition';
    kafkaOffset      @title: 'Offset';
    payloadSize      @title: 'Payload Size';
    headersSize      @title: 'Headers Size';
    truncated        @title: 'Truncated';
    parseStatus      @title: 'Parse Status';
    tenantId         @title: 'Tenant ID';
    useCaseName      @title: 'Use Case';
    serviceType      @title: 'Service Type';
    calmAction       @title: 'Action';
    agentVersion     @title: 'Agent Version';
    payload          @title: 'Payload'  @UI.MultiLineText;
    properties       @title: 'Properties'  @UI.MultiLineText;
    ingestedAt       @title: 'Ingested At';
}

annotate MonitoringService.ServiceOverview with @(
    UI: {
        HeaderInfo         : {
            $Type         : 'UI.HeaderInfoType',
            TypeName      : 'Service',
            TypeNamePlural: 'Services',
            Title         : {Value: serviceName}
        },
        PresentationVariant: {
            $Type    : 'UI.PresentationVariantType',
            SortOrder: [{
                $Type     : 'Common.SortOrderType',
                Property  : messageCount,
                Descending: true
            }],
            Visualizations: ['@UI.LineItem']
        },
        SelectionFields    : [serviceName],
        LineItem           : [
            {
                $Type: 'UI.DataField',
                Value: serviceName
            },
            {
                $Type: 'UI.DataField',
                Value: messageCount
            },
            {
                $Type: 'UI.DataField',
                Value: firstMessageAt
            },
            {
                $Type: 'UI.DataField',
                Value: lastMessageAt
            },
            {
                $Type: 'UI.DataField',
                Value: totalPayloadSize
            }
        ]
    }
);

annotate MonitoringService.ServiceOverview with {
    serviceName      @title: 'Service Name';
    messageCount     @title: 'Messages';
    firstMessageAt   @title: 'First Message';
    lastMessageAt    @title: 'Last Message';
    totalPayloadSize @title: 'Total Payload Size';
}
