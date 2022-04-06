require "spec_helper"

describe Tracklib do
  it "can write an I64 column" do
    section = Tracklib::Section.new(:standard, [["a", :i64]], [{"a" => 0},
                                                               {},
                                                               {"a" => 40},
                                                               {"a" => -40}])
    expect(Tracklib::write_track([], [section])
             .unpack("C*")[27..])
      .to eq([# Data Table

               0x01, # one data section

               # Data Table Section 1
               0x00, # data encoding = standard
               0x04, # point count
               0x10, # leb128 data size

               # Schema for Section 1
               0x00, # schema version
               0x01, # field count
               0x00, # first field type = I64
               0x01, # field name length
               0x61, # field name = "a"
               0x08, # leb128 data size

               # Data Table CRC
               0x8A,
               0x59,

               # Data Section 1

               # Presence Column
               0b00000001,
               0b00000000,
               0b00000001,
               0b00000001,
               0x58, # crc
               0x64,
               0x4E,
               0x32,

               # Data Column 1 = "a"
               0x00, # 0
               0x28, # 40
               0xB0, # -40
               0x7F,
               0xAB, # crc
               0x03,
               0xAE,
               0x67])
  end

  it "can write a Bool column" do
    section = Tracklib::Section.new(:standard, [["a", :bool]], [{"a" => true},
                                                                {},
                                                                {"a" => false}])
    expect(Tracklib::write_track([], [section])
             .unpack("C*")[27..])
      .to eq([# Data Table

               0x01, # one data section

               # Data Table Section 1
               0x00, # data encoding = standard
               0x03, # point count
               0x0D, # leb128 data size

               # Schema for Section 1
               0x00, # schema version
               0x01, # field count
               0x05, # first field type = Bool
               0x01, # field name length
               0x61, # field name = "a"
               0x06, # leb128 data size

               # Data Table CRC
               0x87,
               0xB6,

               # Data Section 1

               # Presence Column
               0b00000001,
               0b00000000,
               0b00000001,
               0xCF, # crc
               0x33,
               0x82,
               0x4D,

               # Data Column 1 = "a"
               0x01, # true
               0x00, # false
               0x5E, # crc
               0x5A,
               0x51,
               0x2D])
  end

  it "can write a String column" do
    section = Tracklib::Section.new(:standard, [["a", :string]], [{"a" => "RWGPS"},
                                                                  {},
                                                                  {"a" => "Supercalifragilisticexpialidocious"}])
    expect(Tracklib::write_track([], [section])
             .unpack("C*")[27..])
      .to eq([# Data Table

               0x01, # one data section

               # Data Table Section 1
               0x00, # data encoding = standard
               0x03, # point count
               0x34, # leb128 data size

               # Schema for Section 1
               0x00, # schema version
               0x01, # field count
               0x04, # first field type = String
               0x01, # field name length
               0x61, # field name = "a"
               0x2D, # leb128 data size

               # Data Table CRC
               0x6F,
               0x56,

               # Data Section 1

               # Presence Column
               0b00000001,
               0b00000000,
               0b00000001,
               0xCF, # crc
               0x33,
               0x82,
               0x4D,

               # Data Column 1 = "a"
               0x05, # length 5
               0x52, # R
               0x57, # W
               0x47, # G
               0x50, # P
               0x53, # S
               0x22, # length 34
               0x53, # S
               0x75, # u
               0x70, # p
               0x65, # e
               0x72, # r
               0x63, # c
               0x61, # a
               0x6C, # l
               0x69, # i
               0x66, # f
               0x72, # r
               0x61, # a
               0x67, # g
               0x69, # i
               0x6C, # l
               0x69, # i
               0x73, # s
               0x74, # t
               0x69, # i
               0x63, # c
               0x65, # e
               0x78, # x
               0x70, # p
               0x69, # i
               0x61, # a
               0x6C, # l
               0x69, # i
               0x64, # d
               0x6F, # o
               0x63, # c
               0x69, # i
               0x6F, # o
               0x75, # u
               0x73, # s
               0xC2, # crc
               0x88,
               0x97,
               0xF3])
  end

  it "can write an F64 column" do
    section = Tracklib::Section.new(:standard, [["a", :f64]], [{"a" => 0.0003},
                                                               {},
                                                               {"a" => -27.2}])
    expect(Tracklib::write_track([], [section])
             .unpack("C*")[27..])
      .to eq([# Data Table

               0x01, # one data section

               # Data Table Section 1
               0x00, # data encoding = standard
               0x03, # point count
               0x12, # leb128 data size

               # Schema for Section 1
               0x00, # schema version
               0x01, # field count
               0x01, # first field type = F64
               0x01, # field name length
               0x61, # field name = "a"
               0x0B, # leb128 data size

               # Data Table CRC
               0xA9,
               0x82,

               # Data Section 1

               # Presence Column
               0b00000001,
               0b00000000,
               0b00000001,
               0xCF, # crc
               0x33,
               0x82,
               0x4D,

               # Data Column 1 = "a"
               0xB7, # first val
               0x17,
               0xC9, # second val
               0xA0,
               0xA6,
               0xFE,
               0x7E,

               0xAF, # crc
               0x4E,
               0x38,
               0xBE])
  end

  it "can convert floats and integers" do
    section = Tracklib::Section.new(:standard, [["i", :i64], ["f", :f64]], [{"i" => 1, "f" => 0.0},
                                                                            {"i" => 1.0, "f" => 5},
                                                                            {},
                                                                            {"i" => 2.6}])
    expect(Tracklib::write_track([], [section])
             .unpack("C*")[27..])
      .to eq([# Data Table

               0x01, # one data section

               # Data Table Section 1
               0x00, # data encoding = standard
               0x04, # point count
               0x18, # leb128 data size

               # Schema for Section 1
               0x00, # schema version
               0x02, # field count
               0x00, # first field type = I64
               0x01, # field name length
               0x69, # field name = "i"
               0x07, # leb128 data size
               0x01, # second field type = F64
               0x01, # field name length
               0x66, # field name = "f"
               0x09, # leb128 data size

               # Data Table CRC
               0xD5,
               0x93,

               # Data Section 1

               # Presence Column
               0b00000011,
               0b00000011,
               0b00000000,
               0b00000001,
               0xD4, # crc
               0x78,
               0x24,
               0x5E,

               # Data Column 1 = "i"
               0x01, # start at 1
               0x00, # stay at 1
               0x02, # increment by 2 to get 3

               0x16, # crc
               0x15,
               0xC1,
               0x40,

               # Data Column 2 = "f"
               0x00, # 0
               0x80, # 5
               0xE1,
               0xEB,
               0x17,

               0x5F, # crc
               0x32,
               0x25,
               0xF7])
  end

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
