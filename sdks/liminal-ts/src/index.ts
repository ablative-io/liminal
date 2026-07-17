export { Channel, SdkError, createChannel } from "./channel.js";
export type {
  ChannelConfig,
  ChannelTransport,
  JsonSchema,
  PublishResult,
  RequestReplyMetadata,
  RequestReplyOptions,
  SchemaValidator,
  SdkErrorCode,
  ValidationIssue,
  ValidationResult,
} from "./channel.js";

export {
  accept,
  defer,
  fromTransportResult,
  isAccept,
  isDefer,
  isReject,
  kindForState,
  reject,
  withPressure,
} from "./pressure.js";
export type {
  PressureAccept,
  PressureChannel,
  PressureDefer,
  PressureKind,
  PressureReject,
  PressureResponse,
  PressureState,
} from "./pressure.js";

export { openConversation } from "./conversation.js";
export type {
  Conversation,
  ConversationConfig,
  ConversationError,
  ConversationErrorCode,
  ConversationEvent,
  ConversationEventKind,
  ConversationTransport,
  TerminateReason,
} from "./conversation.js";

export { backoffDelay, Connection } from "./connection.js";
export type {
  ConnectionConfig,
  ConnectionState,
  ConnectionStateChange,
  ConnectionStateListener,
  ConnectionTransport,
  DisconnectReason,
  SubscriptionCursor,
} from "./connection.js";

export { generate } from "./codegen.js";
export type { ChannelDefinition, GenerateOptions } from "./codegen.js";

export {
  DEFAULT_FEED_CHANNEL,
  LiminalFeedSource,
  RESERVED_OBSERVABILITY_CHANNEL,
} from "./feed-source.js";
export { LiminalFeedSourceError } from "./feed-source-error.js";
export type {
  FeedSource,
  LiminalFeedSourceErrorListener,
  LiminalFeedSourceOptions,
} from "./feed-source.js";
export type { LiminalFeedSourceErrorCode } from "./feed-source-error.js";
