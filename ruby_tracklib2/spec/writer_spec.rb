require "spec_helper"

describe Tracklib do
  it "can write an I64 column" do
    schema = Tracklib::Schema.new([["a", :i64]])
    section = Tracklib::Section.new(:standard, schema, [{"a" => 0},
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
               0x00, # field type = I64
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

  it "can write an F64 column" do
    schema = Tracklib::Schema.new([["a", :f64, 7]])
    section = Tracklib::Section.new(:standard, schema, [{"a" => 0.0003},
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
               0x01, # field type = F64
               0x07, # scale
               0x01, # field name length
               0x61, # field name = "a"
               0x0B, # leb128 data size

               # Data Table CRC
               0x6D,
               0x30,

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

  it "can write an U64 column" do
    schema = Tracklib::Schema.new([["a", :u64]])
    section = Tracklib::Section.new(:standard, schema, [{"a" => 0},
                                                        {},
                                                        {"a" => 40},
                                                        {"a" => 10}])
    expect(Tracklib::write_track([], [section])
             .unpack("C*")[27..])
      .to eq([# Data Table

               0x01, # one data section

               # Data Table Section 1
               0x00, # data encoding = standard
               0x04, # point count
               0x0F, # leb128 data size

               # Schema for Section 1
               0x00, # schema version
               0x01, # field count
               0x02, # field type = U64
               0x01, # field name length
               0x61, # field name = "a"
               0x07, # leb128 data size

               # Data Table CRC
               0x25,
               0x24,

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
               0x62, # -30
               0x72, # crc
               0x0A,
               0x57,
               0x47])
  end

  it "can write a Bool column" do
    schema = Tracklib::Schema.new([["a", :bool]])
    section = Tracklib::Section.new(:standard, schema, [{"a" => true},
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
               0x10, # field type = Bool
               0x01, # field name length
               0x61, # field name = "a"
               0x06, # leb128 data size

               # Data Table CRC
               0x83,
               0xBA,

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
    schema = Tracklib::Schema.new([["a", :string]])
    section = Tracklib::Section.new(:standard, schema, [{"a" => "RWGPS"},
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
               0x20, # field type = String
               0x01, # field name length
               0x61, # field name = "a"
               0x2D, # leb128 data size

               # Data Table CRC
               0x65,
               0xA6,

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

  it "can write a BoolArray column" do
    schema = Tracklib::Schema.new([["a", :bool_array]])
    section = Tracklib::Section.new(:standard, schema, [{"a" => [true, false, false]},
                                                        {},
                                                        {"a" => []}])
    expect(Tracklib::write_track([], [section])
             .unpack("C*")[27..])
      .to eq([# Data Table

               0x01, # one data section

               # Data Table Section 1
               0x00, # data encoding = standard
               0x03, # point count
               0x10, # leb128 data size

               # Schema for Section 1
               0x00, # schema version
               0x01, # field count
               0x21, # field type = BoolArray
               0x01, # field name length
               0x61, # field name = "a"
               0x09, # leb128 data size

               # Data Table CRC
               0x00,
               0x43,

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
               0x03, # array len 3
               0x01, # true
               0x00, # false
               0x00, # false
               0x00, # array len 0,
               0x43,
               0x76,
               0x95,
               0xBF])
  end

  it "can write a U64Array column" do
    schema = Tracklib::Schema.new([["a", :u64_array]])
    section = Tracklib::Section.new(:standard, schema, [{"a" => [99, 98, 500]},
                                                        {},
                                                        {"a" => []}])
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
               0x22, # field type = U64Array
               0x01, # field name length
               0x61, # field name = "a"
               0x0B, # leb128 data size

               # Data Table CRC
               0xA2,
               0x06,

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
               0x03, # array len 3
               0xE3, # 99
               0x00,
               0x7F, # -1
               0x92, # 98
               0x03,
               0x00, # array len 0
               0xF1, # crc
               0x29,
               0x76,
               0x36])
  end

  it "can write a ByteArray column" do
    schema = Tracklib::Schema.new([["a", :byte_array]])
    section = Tracklib::Section.new(:standard, schema, [{"a" => "RWGPS"},
                                                        {},
                                                        {"a" => ""}])
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
               0x23, # field type = ByteArray
               0x01, # field name length
               0x61, # field name = "a"
               0x0B, # leb128 data size

               # Data Table CRC
               0xA3,
               0xFA,

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
               0x05, # array len 5
               0x52, # R
               0x57, # W
               0x47, # G
               0x50, # P
               0x53, # S
               0x00, # array len 0
               0x6A, # crc
               0x29,
               0x93,
               0xA3])
  end

  it "can convert floats and integers" do
    schema = Tracklib::Schema.new([["i", :i64],
                                   ["u", :u64],
                                   ["f", :f64, 7],
                                   ["ua", :u64_array]])
    section = Tracklib::Section.new(:standard,
                                    schema,
                                    [{"i" => -1, "u" => 1, "f" => 0.0, "ua" => [0, 1.0, 1.2, 1.9, 5]},
                                     {"i" => 1.0, "u"=> 3.0, "f" => 5},
                                     {},
                                     {"i" => -2.6, "u" => 32.21}])
    expect(Tracklib::write_track([], [section])
             .unpack("C*")[27..])
      .to eq([# Data Table

               0x01, # one data section

               # Data Table Section 1
               0x00, # data encoding = standard
               0x04, # point count
               0x29, # leb128 data size

               # Schema for Section 1
               0x00, # schema version
               0x04, # field count
               0x00, # first field type = I64
               0x01, # field name length
               0x69, # field name = "i"
               0x07, # leb128 data size
               0x02, # second field type = U64
               0x01, # field name length
               0x75, # field name = "u"
               0x07, # leb128 data size
               0x01, # third field type = F64
               0x07, # scale
               0x01, # field name length
               0x66, # field name = "f"
               0x09, # leb128 data size
               0x22, # fourth field type = U64Array
               0x02, # field name length
               0x75, # field name = "ua"
               0x61,
               0x0A, # leb128 data size

               # Data Table CRC
               0x8D,
               0x59,

               # Data Section 1

               # Presence Column
               0b00001111,
               0b00000111,
               0b00000000,
               0b00000011,
               0xA9, # crc
               0x25,
               0xDB,
               0xD5,

               # Data Column 1 = "i"
               0x7F, # -1
               0x02, # +2
               0x7C, # -4
               0x8E, # crc
               0xC0,
               0xAB,
               0x66,

               # Data Column 2 = "u"
               0x01, # 1
               0x02, # +2
               0x1D, # +29
               0xD4, # crc
               0xED,
               0x6D,
               0x94,

               # Data Column 3 = "f"
               0x00,
               0x80,
               0xE1,
               0xEB,
               0x17,
               0x5F, # crc
               0x32,
               0x25,
               0xF7,

               # Data Column 4 = "ua"
               0x05, # array len
               0x00, # 0
               0x01, # +1
               0x00, # no change
               0x01, # +1
               0x03, # +3
               0xA7, # crc
               0x06,
               0x3A,
               0xB1])
  end

  it "raises errors for invalid F64 scale" do
    expect { Tracklib::Schema.new([["a", :f64]]) }.to raise_error
    expect { Tracklib::Schema.new([["a", :f64, "2"]]) }.to raise_error
    expect { Tracklib::Schema.new([["a", :f64, 500]]) }.to raise_error
  end

  it "raises errors for invalid array type elements" do
    expect {
      Tracklib::Section.new(:standard,
                            Tracklib::Schema.new([["a", :bool_array]]),
                            [{"a" => [true, false, 0]}])
    }.to raise_error

    expect {
      Tracklib::Section.new(:standard,
                            Tracklib::Schema.new([["a", :u64_array]]),
                            [{"a" => [0, 1, 2, "3"]}])
    }.to raise_error
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
      .to eq([0x01, # metadata table len
              0x00, # entry type: track_type
              0x02, # entry size
              0x01, # track type: route
              0x40, # route id
              0x47, # crc
              0x9F,

              0x00, # empty data table
              0x40,
              0xBF])

    # created_at
    expect(Tracklib::write_track([[:created_at, Time.new(1970, 1, 1, 0, 0, 0, "UTC")]], [])
             .unpack("C*")[24..])
      .to eq([0x01, # metadata table len = 1
              0x01, # entry type: created_at
              0x01, # entry size
              0x00, # timestamp
              0xAE, # crc
              0x77,

              0x00, # empty data table
              0x40,
              0xBF])

    # both
    expect(Tracklib::write_track([[:track_type, :trip, 20],
                                  [:created_at, Time.new(1970, 1, 1, 0, 0, 0, "UTC")]],
                                 [])
             .unpack("C*")[24..])
      .to eq([0x02, # two metadata entries
              0x00, # entry type: track_type
              0x02, # entry size
              0x00, # track type: trip
              0x14, # trip id
              0x01, # entry type: created_at
              0x01, # entry size
              0x00, # timestamp
              0x6A, # crc
              0x6F,

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
