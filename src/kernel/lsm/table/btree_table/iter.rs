use crate::kernel::lsm::iterator::{Iter, Seek, SeekIter};
use crate::kernel::lsm::mem_table::KeyValue;
use crate::kernel::lsm::table::btree_table::BTreeTable;
use bytes::Bytes;
use std::collections::btree_map::Range;
use std::collections::Bound;

pub(crate) struct BTreeTableIter<'a> {
    inner: Option<Range<'a, Bytes, KeyValue>>,
    table: &'a BTreeTable,
}

impl<'a> BTreeTableIter<'a> {
    pub(crate) fn new(table: &'a BTreeTable) -> BTreeTableIter<'a> {
        let mut iter = BTreeTableIter { inner: None, table };
        iter._seek(Seek::First);
        iter
    }

    fn _seek(&mut self, seek: Seek) {
        self.inner = match seek {
            Seek::First => Some(
                self.table
                    .inner
                    .range::<Bytes, (Bound<Bytes>, Bound<Bytes>)>((
                        Bound::Unbounded,
                        Bound::Unbounded,
                    )),
            ),
            Seek::Last => None,
            Seek::Backward(key) => Some(
                self.table
                    .inner
                    .range::<Bytes, (Bound<Bytes>, Bound<Bytes>)>((
                        Bound::Included(Bytes::copy_from_slice(key)),
                        Bound::Unbounded,
                    )),
            ),
        };
    }
}

impl<'a> Iter<'a> for BTreeTableIter<'a> {
    type Item = KeyValue;

    fn try_next(&mut self) -> crate::kernel::KernelResult<Option<Self::Item>> {
        Ok(self
            .inner
            .as_mut()
            .and_then(|iter| iter.next())
            .map(item_clone))
    }

    fn is_valid(&self) -> bool {
        true
    }
}

impl<'a> SeekIter<'a> for BTreeTableIter<'a> {
    fn seek(&mut self, seek: Seek<'_>) -> crate::kernel::KernelResult<()> {
        self._seek(seek);

        Ok(())
    }
}

fn item_clone((_, value): (&Bytes, &KeyValue)) -> KeyValue {
    value.clone()
}

#[cfg(test)]
mod tests {
    use crate::kernel::lsm::iterator::Seek;
    use crate::kernel::lsm::table::btree_table::BTreeTable;
    use crate::kernel::lsm::table::Table;
    use crate::kernel::KernelResult;
    use bytes::Bytes;

    #[test]
    fn test_iterator() -> KernelResult<()> {
        let vec = vec![
            (Bytes::from(vec![b'1']), None),
            (Bytes::from(vec![b'2']), Some(Bytes::from(vec![b'1']))),
            (Bytes::from(vec![b'3']), None),
            (Bytes::from(vec![b'4']), None),
            (Bytes::from(vec![b'5']), Some(Bytes::from(vec![b'2']))),
            (Bytes::from(vec![b'6']), None),
        ];
        let table = BTreeTable::new(0, 0, vec.clone());
        let mut iter = table.iter()?;

        for test_data in vec.clone() {
            assert_eq!(iter.try_next()?, Some(test_data))
        }

        assert_eq!(iter.try_next()?, None);

        iter.seek(Seek::First)?;
        assert_eq!(iter.try_next()?, Some(vec[0].clone()));

        iter.seek(Seek::Backward(&[b'3']))?;
        assert_eq!(iter.try_next()?, Some(vec[2].clone()));

        iter.seek(Seek::Last)?;
        assert_eq!(iter.try_next()?, None);

        Ok(())
    }
}
