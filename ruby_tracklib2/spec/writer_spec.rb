require "spec_helper"

describe Tracklib::Section do
  it "can write metadata" do
    # empty metadata
    expect(Tracklib::write_track([], [])
             .unpack("C*")[24..])
      .to eq([0x00, # empty metadata table
              0x40,
              0xBF,

              0x00, # empty data table
              0x40,
              0xBF])

    # track_type
    expect(Tracklib::write_track([[:track_type, :route, 64]], [])
             .unpack("C*")[24..])
      .to eq([0x01, # metadata table len = 1
              0x00, # entry type = track_type
              0x05, # two byte entry size = 5
              0x00,
              0x01, # track type: route = 0x01
              0x40, # four byte route ID = 64
              0x00,
              0x00,
              0x00,
              0x85, # crc
              0x9F,

              0x00, # empty data table
              0x40,
              0xBF])

    # created_at
    expect(Tracklib::write_track([[:created_at, Time.new(1970, 1, 1, 0, 0, 0, "UTC")]], [])
             .unpack("C*")[24..])
      .to eq([0x01, # metadata table len = 1
              0x01, # entry type = created_at
              0x08, # two byte entry size = 8
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
              0xE3, # crc
              0x28,

              0x00, # empty data table
              0x40,
              0xBF])

    # both
    expect(Tracklib::write_track([[:track_type, :trip, 20],
                                  [:created_at, Time.new(1970, 1, 1, 0, 0, 0, "UTC")]],
                                 [])
             .unpack("C*")[24..])
      .to eq([0x02, # two metadata entries
              0x00, # entry type: track_type = 0x00
              0x05, # two byte entry size = 5
              0x00,
              0x00, # track type: trip = 0x00
              0x14, # four byte trip ID = 20
              0x00,
              0x00,
              0x00,
              0x01, # entry type: created_at = 0x01
              0x08, # two byte entry size = 8
              0x00,
              0x00, # eight byte timestamp: zero seconds elapsed
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
              0x00,
              0x23, # crc
              0xD2,

              0x00, # empty data table
              0x40,
              0xBF])

    # unknown type
    expect { Tracklib::write_track([[:foo, 25]], []) } .to raise_error "Metadata Type 'foo' unknown"

    # invalid length
    expect { Tracklib::write_track([[:created_at, Time.now, "foo"]], []) } .to raise_error "Metadata Entries for 'created_at' must have length 2"

    # invalid args
    expect { Tracklib::write_track([[]], []) } .to raise_error "Invalid Metadata Entry"
    expect { Tracklib::write_track(["Foo"], []) } .to raise_error "Error converting to Array"
    expect { Tracklib::write_track([[:created_at, "foo"]], []) } .to raise_error "Error converting to Time"
    expect { Tracklib::write_track([[:track_type, :foo, 5]], []) } .to raise_error "Metadata Entry Track Type 'foo' unknown"
    expect { Tracklib::write_track([[:track_type, :route, "foo"]], []) } .to raise_error "Error converting to Integer"
  end
end
