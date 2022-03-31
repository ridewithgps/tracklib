use ouroboros::self_referencing;
use rutie::{
    class, methods, wrappable_struct, AnyObject, Array, Class, Integer, Object, RString, Symbol, VM,
};
use tracklib2;

#[self_referencing]
pub struct TrackReaderWrapper {
    data: Vec<u8>,
    #[borrows(data)]
    #[not_covariant]
    track_reader: tracklib2::read::track::TrackReader<'this>,
}

wrappable_struct!(
    TrackReaderWrapper,
    TrackReaderWrapperWrapper,
    TRACK_READER_WRAPPER
);

class!(TrackReader);

methods!(
    TrackReader,
    rtself,
    fn trackreader_new(bytes: RString) -> AnyObject {
        let source = bytes.map_err(|e| VM::raise_ex(e)).unwrap();
        let data = source.to_bytes_unchecked().to_vec();
        let wrapper = TrackReaderWrapper::new(data, |d| {
            tracklib2::read::track::TrackReader::new(d)
                .map_err(|e| VM::raise(Class::from_existing("Exception"), &format!("{}", e)))
                .unwrap()
        });

        Class::from_existing("TrackReader").wrap_data(wrapper, &*TRACK_READER_WRAPPER)
    },
    fn trackreader_metadata() -> Array {
        let metadata_entries = rtself
            .get_data(&*TRACK_READER_WRAPPER)
            .with_track_reader(|track_reader| track_reader.metadata());

        let mut metadata_array = Array::new();

        for metadata_entry in metadata_entries {
            let metadata_entry_array = match metadata_entry {
                tracklib2::types::MetadataEntry::TrackType(track_type) => {
                    let mut metadata_entry_array = Array::new();

                    let (type_name, id) = match track_type {
                        tracklib2::types::TrackType::Trip(id) => {
                            (Symbol::new("trip"), Integer::from(*id))
                        }
                        tracklib2::types::TrackType::Route(id) => {
                            (Symbol::new("route"), Integer::from(*id))
                        }
                        tracklib2::types::TrackType::Segment(id) => {
                            (Symbol::new("segment"), Integer::from(*id))
                        }
                    };

                    metadata_entry_array.push(Symbol::new("track_type"));
                    metadata_entry_array.push(type_name);
                    metadata_entry_array.push(id);

                    metadata_entry_array
                }
                tracklib2::types::MetadataEntry::CreatedAt(created_at) => {
                    let mut metadata_entry_array = Array::new();

                    metadata_entry_array.push(Symbol::new("created_at"));

                    let time_obj = Class::from_existing("Time")
                        .protect_send("at", &[Integer::from(*created_at).to_any_object()])
                        .map_err(|e| VM::raise_ex(e))
                        .unwrap()
                        .protect_send("utc", &[])
                        .map_err(|e| VM::raise_ex(e))
                        .unwrap();

                    metadata_entry_array.push(time_obj);

                    metadata_entry_array
                }
            };

            metadata_array.push(metadata_entry_array);
        }

        metadata_array
    }
);
