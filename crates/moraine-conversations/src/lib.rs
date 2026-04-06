mod clickhouse_repo;
mod cursor;
mod domain;
mod error;
mod repo;

pub use clickhouse_repo::ClickHouseConversationRepository;
pub use domain::{
    is_user_facing_content_event, Conversation, ConversationDetailOptions, ConversationListFilter,
    ConversationMode, ConversationSearchHit, ConversationSearchQuery, ConversationSearchResults,
    ConversationSearchStats, ConversationSummary, OpenContext, OpenEvent, OpenEventRequest, Page,
    PageRequest, RepoConfig, SearchEventHit, SearchEventKind, SearchEventsQuery,
    SearchEventsResult, SearchEventsStats, SearchEventsStrategy, TraceEvent, Turn, TurnListFilter,
    TurnSummary,
};
pub use error::{RepoError, RepoResult};
pub use repo::ConversationRepository;
