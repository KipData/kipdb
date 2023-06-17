use crate::kernel::lsm::mem_table::{key_value_bytes_len, KeyValue};

pub(crate) trait Trigger {
    fn item_process(&mut self, item: &KeyValue);

    fn is_exceeded(&self) -> bool;
}

#[derive(Copy, Clone)]
pub(crate) struct CountTrigger {
    item_count: usize,
    threshold: usize,
}

impl Trigger for CountTrigger {
    fn item_process(&mut self, _item: &KeyValue) {
        self.item_count += 1;
    }

    fn is_exceeded(&self) -> bool {
        self.item_count >= self.threshold
    }
}

#[derive(Copy, Clone)]
pub(crate) struct SizeOfMemTrigger {
    size_of_mem: usize,
    threshold: usize,
}

impl Trigger for SizeOfMemTrigger {
    fn item_process(&mut self, item: &KeyValue) {
        self.size_of_mem += key_value_bytes_len(item);
    }

    fn is_exceeded(&self) -> bool {
        self.size_of_mem >= self.threshold
    }
}

#[derive(Copy, Clone, Debug)]
pub enum TriggerType {
    Count,
    SizeOfMem,
}

pub(crate) struct TriggerFactory { }

impl TriggerFactory {
    pub(crate) fn create(trigger_type: TriggerType, threshold: usize) -> Box<dyn Trigger + Send> {
        match trigger_type {
            TriggerType::Count => Box::new(CountTrigger { item_count: 0, threshold }),
            TriggerType::SizeOfMem => Box::new(SizeOfMemTrigger { size_of_mem: 0, threshold })
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use crate::kernel::lsm::trigger::{TriggerFactory, TriggerType};

    #[test]
    fn test_count_trigger() {
        let mut trigger = TriggerFactory::create(TriggerType::Count, 2);

        trigger.item_process(&(Bytes::new(), None));
        assert!(!trigger.is_exceeded());

        trigger.item_process(&(Bytes::new(), None));
        assert!(trigger.is_exceeded());
    }

    #[test]
    fn test_size_of_mem_trigger() {
        let mut trigger = TriggerFactory::create(TriggerType::SizeOfMem, 2);

        trigger.item_process(&(Bytes::from(vec![b'0']), None));
        assert!(!trigger.is_exceeded());

        trigger.item_process(&(Bytes::from(vec![b'0']), None));
        assert!(trigger.is_exceeded());
    }
}
