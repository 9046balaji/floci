package io.github.hectorvent.floci.services.apigateway;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.services.acm.AcmJsonHandler;
import io.github.hectorvent.floci.services.cloudwatch.logs.CloudWatchLogsHandler;
import io.github.hectorvent.floci.services.cloudwatch.metrics.CloudWatchMetricsJsonHandler;
import io.github.hectorvent.floci.services.cognito.CognitoJsonHandler;
import io.github.hectorvent.floci.services.dynamodb.DynamoDbJsonHandler;
import io.github.hectorvent.floci.services.eventbridge.EventBridgeHandler;
import io.github.hectorvent.floci.services.kinesis.KinesisJsonHandler;
import io.github.hectorvent.floci.services.kms.KmsJsonHandler;
import io.github.hectorvent.floci.services.secretsmanager.SecretsManagerJsonHandler;
import io.github.hectorvent.floci.services.sns.SnsJsonHandler;
import io.github.hectorvent.floci.services.sqs.SqsJsonHandler;
import io.github.hectorvent.floci.services.ssm.SsmJsonHandler;
import io.github.hectorvent.floci.services.stepfunctions.StepFunctionsJsonHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response;
import org.jboss.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Routes API Gateway AWS integration requests to the correct internal service handler.
 *
 * <p>Parses integration URIs of the form
 * {@code arn:aws:apigateway:{region}:{service}:action/{ActionName}}
 * and dispatches to the matching JSON handler.
 */
@ApplicationScoped
public class AwsServiceRouter {

    private static final Logger LOG = Logger.getLogger(AwsServiceRouter.class);

    private final StepFunctionsJsonHandler stepFunctionsHandler;
    private final DynamoDbJsonHandler dynamoDbHandler;
    private final SqsJsonHandler sqsHandler;
    private final SnsJsonHandler snsHandler;
    private final EventBridgeHandler eventBridgeHandler;
    private final SsmJsonHandler ssmHandler;
    private final KinesisJsonHandler kinesisHandler;
    private final CloudWatchLogsHandler logsHandler;
    private final CloudWatchMetricsJsonHandler metricsHandler;
    private final SecretsManagerJsonHandler secretsManagerHandler;
    private final KmsJsonHandler kmsHandler;
    private final CognitoJsonHandler cognitoHandler;
    private final AcmJsonHandler acmHandler;

    @Inject
    public AwsServiceRouter(StepFunctionsJsonHandler stepFunctionsHandler,
                            DynamoDbJsonHandler dynamoDbHandler,
                            SqsJsonHandler sqsHandler,
                            SnsJsonHandler snsHandler,
                            EventBridgeHandler eventBridgeHandler,
                            SsmJsonHandler ssmHandler,
                            KinesisJsonHandler kinesisHandler,
                            CloudWatchLogsHandler logsHandler,
                            CloudWatchMetricsJsonHandler metricsHandler,
                            SecretsManagerJsonHandler secretsManagerHandler,
                            KmsJsonHandler kmsHandler,
                            CognitoJsonHandler cognitoHandler,
                            AcmJsonHandler acmHandler) {
        this.stepFunctionsHandler = stepFunctionsHandler;
        this.dynamoDbHandler = dynamoDbHandler;
        this.sqsHandler = sqsHandler;
        this.snsHandler = snsHandler;
        this.eventBridgeHandler = eventBridgeHandler;
        this.ssmHandler = ssmHandler;
        this.kinesisHandler = kinesisHandler;
        this.logsHandler = logsHandler;
        this.metricsHandler = metricsHandler;
        this.secretsManagerHandler = secretsManagerHandler;
        this.kmsHandler = kmsHandler;
        this.cognitoHandler = cognitoHandler;
        this.acmHandler = acmHandler;
    }

    private static final Pattern ACTION_URI_PATTERN = Pattern.compile(
            "^arn:aws:apigateway:([^:]+):([^:]+):action/(.+)$"
    );
    private static final Pattern PATH_URI_PATTERN = Pattern.compile(
            "^arn:aws:apigateway:([^:]+):([^:]+):path/(.+)$"
    );

    /**
     * Parsed components of an API Gateway AWS integration URI.
     */
    public record IntegrationTarget(String region, String service, String action, String path) {
        public IntegrationTarget(String region, String service, String action) {
            this(region, service, action, null);
        }
    }

    /**
     * Parses an integration URI like
     * {@code arn:aws:apigateway:us-east-1:states:action/StartExecution} or
     * {@code arn:aws:apigateway:us-east-1:sqs:path/123456789012/my-queue}.
     *
     * @return parsed target, or null if the URI format is not recognized
     */
    public IntegrationTarget parseIntegrationUri(String uri) {
        if (uri == null || !uri.startsWith("arn:aws:apigateway:")) {
            return null;
        }

        Matcher actionMatcher = ACTION_URI_PATTERN.matcher(uri);
        if (actionMatcher.matches()) {
            return new IntegrationTarget(
                actionMatcher.group(1),
                actionMatcher.group(2),
                actionMatcher.group(3),
                null
            );
        }

        Matcher pathMatcher = PATH_URI_PATTERN.matcher(uri);
        if (pathMatcher.matches()) {
            return new IntegrationTarget(
                pathMatcher.group(1),
                pathMatcher.group(2),
                null,
                pathMatcher.group(3)
            );
        }

        return null;
    }

    /**
     * Dispatches to the appropriate service handler.
     *
     * @param service     the AWS service name from the URI (e.g., "states", "dynamodb")
     * @param action      the action name (e.g., "StartExecution", "PutItem")
     * @param requestBody the JSON request body
     * @param region      the AWS region
     * @return the service response
     */
    public Response invoke(String service, String action, JsonNode requestBody, String region) {
        LOG.debugv("AWS integration dispatch: {0}:{1} in {2}", service, action, region);

        try {
            return switch (service) {
                case "states" -> stepFunctionsHandler.handle(action, requestBody, region);
                case "dynamodb" -> dynamoDbHandler.handle(action, requestBody, region);
                case "sqs" -> sqsHandler.handle(action, requestBody, region);
                case "sns" -> snsHandler.handle(action, requestBody, region);
                case "events" -> eventBridgeHandler.handle(action, requestBody, region);
                case "ssm" -> ssmHandler.handle(action, requestBody, region);
                case "kinesis" -> kinesisHandler.handle(action, requestBody, region);
                case "logs" -> logsHandler.handle(action, requestBody, region);
                case "monitoring" -> metricsHandler.handle(action, requestBody, region);
                case "secretsmanager" -> secretsManagerHandler.handle(action, requestBody, region);
                case "kms" -> kmsHandler.handle(action, requestBody, region);
                case "cognito-idp" -> cognitoHandler.handle(action, requestBody, region);
                case "acm" -> acmHandler.handle(action, requestBody, region);
                default -> throw new AwsException("UnknownService",
                        "Unsupported AWS service integration: " + service, 400);
            };
        } catch (AwsException e) {
            throw e;
        } catch (Exception e) {
            throw new AwsException("InternalError",
                    e.getMessage() != null ? e.getMessage() : "Service invocation failed", 500);
        }
    }
}
