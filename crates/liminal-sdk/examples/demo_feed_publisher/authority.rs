use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Generation(u64);

impl Generation {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sequence(u64);

impl Sequence {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
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
}

/// Process-scoped feed authority. It has no wall-clock or retry authority.
#[derive(Debug)]
pub struct FeedAuthority {
    generation: Generation,
    channel_sequences: BTreeMap<String, u64>,
}

impl FeedAuthority {
    /// Starts one publisher authority and durably bumps its process generation once.
    pub fn start(store: &mut impl GenerationStore) -> Result<Self, AuthorityError> {
        let previous = store.load()?.map_or(0, std::convert::identity);
        let next = previous
            .checked_add(1)
            .ok_or(AuthorityError::GenerationOverflow)?;
        store.persist(next)?;
        Ok(Self {
            generation: Generation(next),
            channel_sequences: BTreeMap::new(),
        })
    }

    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.generation
    }

    pub fn next_sequence(&mut self, channel: &str) -> Result<Sequence, AuthorityError> {
        let sequence = self
            .channel_sequences
            .entry(channel.to_owned())
            .or_default();
        *sequence = sequence
            .checked_add(1)
            .ok_or_else(|| AuthorityError::SequenceOverflow(channel.to_owned()))?;
        Ok(Sequence(*sequence))
    }
}

#[cfg(test)]
mod tests {
    use super::{FeedAuthority, GenerationStore, GenerationStoreError};

    #[derive(Default)]
    struct MemoryStore {
        value: Option<u64>,
    }

    impl GenerationStore for MemoryStore {
        fn load(&mut self) -> Result<Option<u64>, GenerationStoreError> {
            Ok(self.value)
        }

        fn persist(&mut self, generation: u64) -> Result<(), GenerationStoreError> {
            self.value = Some(generation);
            Ok(())
        }
    }

    #[test]
    fn generation_is_stable_within_run_and_bumps_on_restart()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut store = MemoryStore::default();
        let first = FeedAuthority::start(&mut store)?;
        assert_eq!(first.generation().get(), 1);
        assert_eq!(first.generation(), first.generation());

        let second = FeedAuthority::start(&mut store)?;
        assert_eq!(second.generation().get(), 2);
        Ok(())
    }

    #[test]
    fn sequence_is_monotonic_per_channel() -> Result<(), Box<dyn std::error::Error>> {
        let mut store = MemoryStore::default();
        let mut authority = FeedAuthority::start(&mut store)?;
        assert_eq!(authority.next_sequence("a")?.get(), 1);
        assert_eq!(authority.next_sequence("a")?.get(), 2);
        assert_eq!(authority.next_sequence("b")?.get(), 1);
        Ok(())
    }
}
