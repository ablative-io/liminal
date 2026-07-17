use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Generation(u64);

impl Generation {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Cut {
    generation: Generation,
    seq: u64,
}

impl Cut {
    #[must_use]
    pub const fn generation(self) -> Generation {
        self.generation
    }

    #[must_use]
    pub const fn seq(self) -> u64 {
        self.seq
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GenerationStoreError {
    #[error("generation store I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("generation store contains invalid value {0:?}")]
    Invalid(String),
}

pub trait GenerationStore {
    fn load(&mut self) -> Result<Option<u64>, GenerationStoreError>;
    fn persist(&mut self, generation: u64) -> Result<(), GenerationStoreError>;
}

#[derive(Debug)]
pub struct FileGenerationStore {
    path: PathBuf,
}

impl FileGenerationStore {
    #[must_use]
    pub const fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl GenerationStore for FileGenerationStore {
    fn load(&mut self) -> Result<Option<u64>, GenerationStoreError> {
        match fs::read_to_string(&self.path) {
            Ok(contents) => {
                let value = contents
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| GenerationStoreError::Invalid(contents))?;
                Ok(Some(value))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn persist(&mut self, generation: u64) -> Result<(), GenerationStoreError> {
        let temporary = temporary_path(&self.path);
        let mut file = File::create(&temporary)?;
        writeln!(file, "{generation}")?;
        file.sync_all()?;
        fs::rename(temporary, &self.path)?;
        Ok(())
    }
}

fn temporary_path(path: &Path) -> PathBuf {
    let mut temporary = path.as_os_str().to_owned();
    temporary.push(".tmp");
    temporary.into()
}

#[derive(Debug, thiserror::Error)]
pub enum AuthorityError {
    #[error(transparent)]
    Store(#[from] GenerationStoreError),
    #[error("feed generation overflow")]
    GenerationOverflow,
    #[error("sequence overflow for channel {0:?}")]
    SequenceOverflow(String),
    #[error("channel {0:?} already minted its baseline in this generation")]
    BaselineAlreadyMinted(String),
    #[error("channel {0:?} cannot mint a delta before its baseline")]
    BaselineMissing(String),
}

/// Feed authority for monotonic, non-reusable `(generation, seq)` cuts.
/// It has no wall-clock, delivery, reconnect, or retry authority.
pub struct FeedAuthority {
    generation: Generation,
    channel_sequences: BTreeMap<String, u64>,
    store: Box<dyn GenerationStore>,
}

impl FeedAuthority {
    /// Starts one process authority and durably mints its first generation once.
    pub fn start(mut store: impl GenerationStore + 'static) -> Result<Self, AuthorityError> {
        let previous = store.load()?.map_or(0, std::convert::identity);
        let next = previous
            .checked_add(1)
            .ok_or(AuthorityError::GenerationOverflow)?;
        store.persist(next)?;
        Ok(Self {
            generation: Generation(next),
            channel_sequences: BTreeMap::new(),
            store: Box::new(store),
        })
    }

    /// Mints the unique baseline snapshot cut `(generation, 0)` for `channel`.
    pub fn mint_baseline(&mut self, channel: &str) -> Result<Cut, AuthorityError> {
        if self.channel_sequences.contains_key(channel) {
            return Err(AuthorityError::BaselineAlreadyMinted(channel.to_owned()));
        }
        self.channel_sequences.insert(channel.to_owned(), 0);
        Ok(Cut {
            generation: self.generation,
            seq: 0,
        })
    }

    /// Mints the next delta cut after an existing channel baseline.
    pub fn mint_delta(&mut self, channel: &str) -> Result<Cut, AuthorityError> {
        let sequence = self
            .channel_sequences
            .get_mut(channel)
            .ok_or_else(|| AuthorityError::BaselineMissing(channel.to_owned()))?;
        *sequence = sequence
            .checked_add(1)
            .ok_or_else(|| AuthorityError::SequenceOverflow(channel.to_owned()))?;
        Ok(Cut {
            generation: self.generation,
            seq: *sequence,
        })
    }

    /// Wholesales authority state into the next durable generation and clears baselines.
    pub fn advance_generation(&mut self) -> Result<Generation, AuthorityError> {
        let next = self
            .generation
            .get()
            .checked_add(1)
            .ok_or(AuthorityError::GenerationOverflow)?;
        self.store.persist(next)?;
        self.generation = Generation(next);
        self.channel_sequences.clear();
        Ok(self.generation)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;

    use super::{FeedAuthority, GenerationStore, GenerationStoreError};

    #[derive(Clone, Default)]
    struct MemoryStore {
        value: Rc<Cell<Option<u64>>>,
    }

    impl GenerationStore for MemoryStore {
        fn load(&mut self) -> Result<Option<u64>, GenerationStoreError> {
            Ok(self.value.get())
        }

        fn persist(&mut self, generation: u64) -> Result<(), GenerationStoreError> {
            self.value.set(Some(generation));
            Ok(())
        }
    }

    #[test]
    fn generation_is_stable_within_run_and_bumps_on_restart()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = MemoryStore::default();
        let first = FeedAuthority::start(store.clone())?;
        assert_eq!(first.generation.get(), 1);
        assert_eq!(first.generation, first.generation);

        let second = FeedAuthority::start(store)?;
        assert_eq!(second.generation.get(), 2);
        Ok(())
    }

    #[test]
    fn baseline_is_zero_and_deltas_are_monotonic_per_channel()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut authority = FeedAuthority::start(MemoryStore::default())?;
        let baseline = authority.mint_baseline("a")?;
        assert_eq!((baseline.generation().get(), baseline.seq()), (1, 0));
        assert_eq!(authority.mint_delta("a")?.seq(), 1);
        assert_eq!(authority.mint_delta("a")?.seq(), 2);
        assert_eq!(authority.mint_baseline("b")?.seq(), 0);
        assert!(authority.mint_baseline("a").is_err());
        Ok(())
    }

    #[test]
    fn wholesale_replace_bumps_generation_and_resets_baseline()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut authority = FeedAuthority::start(MemoryStore::default())?;
        let first = authority.mint_baseline("a")?;
        let delta = authority.mint_delta("a")?;
        authority.advance_generation()?;
        let refreshed = authority.mint_baseline("a")?;
        assert!(first < delta);
        assert!(delta < refreshed);
        assert_eq!((refreshed.generation().get(), refreshed.seq()), (2, 0));
        Ok(())
    }
}
