require "spec_helper"

data = [
  # Header
  0x89, # rwtfmagic
  0x52,
  0x57,
  0x54,
  0x46,
  0x0A,
  0x1A,
  0x0A,
  0x01, # file version
  0x00, # fv reserve
  0x00,
  0x00,
  0x00, # creator version
  0x00, # cv reserve
  0x00,
  0x00,
  0x18, # metadata table offset
  0x00,
  0x2E, # data offset
  0x00,
  0x00, # e reserve
  0x00,
  0x8B, # header crc
  0x34,

  # Metadata Table
  0x02, # one entry
  0x00, # entry type: track_type = 0x00
  0x05, # two byte entry size = 5
  0x00,
  0x02, # track type: segment = 0x02
  0x05, # four byte segment ID
  0x00,
  0x00,
  0x00,
  0x01, # entry type: created_at = 0x01
  0x08, # two byte entry size = 8
  0x00,
  0x19, # eight byte timestamp: zero seconds elapsed
  0x00,
  0x00,
  0x00,
  0x00,
  0x00,
  0x00,
  0x00,
  0xB2, # crc
  0xD9,

  # Data Table
  0x02, # two sections

  # Data Table Section 1
  0x00, # section encoding = standard
  0x05, # leb128 point count
  0x84, # leb128 data size
  0x01,

  # Schema for Section 1
  0x00, # schema version
  0x08, # field count
  0x00, # first field type = I64
  0x03, # name len
  'i'.ord, # name
  '6'.ord,
  '4'.ord,
  0x09, # data size
  0x01, # second field type = F64
  0x02, # scale
  0x05, # name len
  'f'.ord, # name
  '6'.ord,
  '4'.ord,
  ':'.ord,
  '2'.ord,
  0x0A, # data len
  0x02, # third field type = U64
  0x03, # name len
  'u'.ord, # name
  '6'.ord,
  '4'.ord,
  0x09, # data len
  0x10, # fourth field type = Bool
  0x04, # name len
  'b'.ord, # name
  'o'.ord,
  'o'.ord,
  'l'.ord,
  0x09, # data len
  0x20, # fifth field type = String
  0x06, # name len
  's'.ord, # name
  't'.ord,
  'r'.ord,
  'i'.ord,
  'n'.ord,
  'g'.ord,
  0x18, # data len
  0x21, # sixth field type = Bool Array
  0x0A, # name len
  'b'.ord, # name
  'o'.ord,
  'o'.ord,
  'l'.ord,
  ' '.ord,
  'a'.ord,
  'r'.ord,
  'r'.ord,
  'a'.ord,
  'y'.ord,
  0x0E, # data len
  0x22, # seventh field type = U64 Array
  0x09, # name len
  'u'.ord, # name
  '6'.ord,
  '4'.ord,
  ' '.ord,
  'a'.ord,
  'r'.ord,
  'r'.ord,
  'a'.ord,
  'y'.ord,
  0x18, # data len
  0x23, # eigth field type = Byte Array
  0x0A, # name len
  'b'.ord, # name
  'y'.ord,
  't'.ord,
  'e'.ord,
  ' '.ord,
  'a'.ord,
  'r'.ord,
  'r'.ord,
  'a'.ord,
  'y'.ord,
  0x18, # data len

  # Data Table Section 2
  0x00, # section encoding = standard
  0x03, # leb128 point count
  0x26, # leb128 data size

  # Schema for Section 2
  0x00, # schema version
  0x03, # field count
  0x00, # first field type = I64
  0x01, # name length
  'a'.ord, # name
  0x07, # leb128 data size
  0x10, # second field type = Bool
  0x01, # name length
  'b'.ord, # name
  0x06, # leb128 data size
  0x20, # third field type = String
  0x01, # name length
  'c'.ord, # name
  0x12, # leb128 data size

  # Data Table CRC
  0xCD,
  0xB9,

  # Data Section 1

  # Presence Column
  0b11111111,
  0b11111111,
  0b11111111,
  0b11111111,
  0b11111111,
  0x4B, # crc
  0xBF,
  0x08,
  0x4E,

  # Data Column 1 = I64
  0x2A, # 42
  0x00, # no change
  0x00, # no change
  0x00, # no change
  0x00, # no change
  0xD0, # crc
  0x8D,
  0x79,
  0x68,

  # Data Column 2 = F64
  0xCE, # 0.78
  0x00,
  0x00,
  0x00,
  0x00,
  0x00,
  0x3C, # crc
  0x2E,
  0x7B,
  0x33,

  # Data Column 3 = U64
  0x19, # 25
  0x00,
  0x00,
  0x00,
  0x00,
  0xE4, # crc
  0x2A,
  0xD9,
  0x33,

  # Data Column 4 = Bool
  0x01, # true
  0x01, # true
  0x01, # true
  0x01, # true
  0x01, # true
  0xB5, # crc
  0xC9,
  0x8F,
  0xFA,

  # Data Column 5 = String
  0x03, # length 3
  'h'.ord,
  'e'.ord,
  'y'.ord,
  0x03, # length 3
  'h'.ord,
  'e'.ord,
  'y'.ord,
  0x03, # length 3
  'h'.ord,
  'e'.ord,
  'y'.ord,
  0x03, # length 3
  'h'.ord,
  'e'.ord,
  'y'.ord,
  0x03, # length 3
  'h'.ord,
  'e'.ord,
  'y'.ord,
  0x36, # crc
  0x71,
  0x24,
  0x0B,

  # Data Column 6 = Bool Array
  0x01, # array len
  0x01, # true
  0x01, # array len
  0x01, # true
  0x01, # array len
  0x01, # true
  0x01, # array len
  0x01, # true
  0x01, # array len
  0x01, # true
  0xB3, # crc
  0x6F,
  0x38,
  0x51,

  # Data Column 7 = U64 Array
  0x03, # array len
  0x0C, # 12
  0x7E, # -2
  0x03, # +3
  0x03, # array len
  0x0C, # 12
  0x7E, # -2
  0x03, # +3
  0x03, # array len
  0x0C, # 12
  0x7E, # -2
  0x03, # +3
  0x03, # array len
  0x0C, # 12
  0x7E, # -2
  0x03, # +3
  0x03, # array len
  0x0C, # 12
  0x7E, # -2
  0x03, # +3
  0xD1, # crc
  0xB4,
  0x14,
  0x37,

  # Data Column 8 = Byte Array
  0x03, # array len
  0x0C, # 12
  0x0A, # 10
  0x0D, # 13
  0x03, # array len
  0x0C, # 12
  0x0A, # 10
  0x0D, # 13
  0x03, # array len
  0x0C, # 12
  0x0A, # 10
  0x0D, # 13
  0x03, # array len
  0x0C, # 12
  0x0A, # 10
  0x0D, # 13
  0x03, # array len
  0x0C, # 12
  0x0A, # 10
  0x0D, # 13
  0x94, # crc
  0x1D,
  0x88,
  0xAB,

  # Data Section 2

  # Presence Column
  0b00000111,
  0b00000101,
  0b00000111,
  0x1A, # crc
  0x75,
  0xEA,
  0xC4,

  # Data Column 1 = I64
  0x01, # 1
  0x01, # 2
  0x02, # 4
  0xCA, # crc
  0xD4,
  0xD8,
  0x92,

  # Data Column 2 = Bool
  0x00, # false
  # None
  0x01, # true
  0x35, # crc
  0x86,
  0x89,
  0xFB,

  # Data Column 3 = String
  0x04, # length 4
  'R'.ord,
  'i'.ord,
  'd'.ord,
  'e'.ord,
  0x04, # length 4
  'w'.ord,
  'i'.ord,
  't'.ord,
  'h'.ord,
  0x03, # length 3
  'G'.ord,
  'P'.ord,
  'S'.ord,
  0xA3, # crc
  0x02,
  0xEC,
  0x48
].pack("c*")

describe Tracklib do
  it "can read metadata" do
    track_reader = Tracklib::TrackReader.new(data)
    expect(track_reader.metadata()).to eq([[:track_type, :segment, 5],
                                           [:created_at, Time.new(1970, 1, 1, 0, 0, 25, "UTC")]])
  end

  it "can read versions" do
    track_reader = Tracklib::TrackReader.new(data)
    expect(track_reader.file_version()).to eq(1)
    expect(track_reader.creator_version()).to eq(0)
  end

  it "can iterate through sections" do
    track_reader = Tracklib::TrackReader.new(data)
    expect(track_reader.section_count()).to eq(2)

    expect(track_reader.section_encoding(0)).to eq(:standard)
    expect(track_reader.section_schema(0)).to eq([["i64", :i64],
                                                  ["f64:2", :f64, 2],
                                                  ["u64", :u64],
                                                  ["bool", :bool],
                                                  ["string", :string],
                                                  ["bool array", :bool_array],
                                                  ["u64 array", :u64_array],
                                                  ["byte array", :byte_array]])
    expect(track_reader.section_rows(0)).to eq(5)
    expect(track_reader.section_data(0)).to eq([{"bool" => true,
                                                 "bool array" => [true],
                                                 "byte array" => [12, 10, 13].pack("C*"),
                                                 "f64:2" => 0.78,
                                                 "i64" => 42,
                                                 "string" => "hey",
                                                 "u64" => 25,
                                                 "u64 array" => [12, 10, 13]},
                                                {"bool" => true,
                                                 "bool array" => [true],
                                                 "byte array" => [12, 10, 13].pack("C*"),
                                                 "f64:2" => 0.78,
                                                 "i64" => 42,
                                                 "string" => "hey",
                                                 "u64" => 25,
                                                 "u64 array" => [12, 10, 13]},
                                                {"bool" => true,
                                                 "bool array" => [true],
                                                 "byte array" => [12, 10, 13].pack("C*"),
                                                 "f64:2" => 0.78,
                                                 "i64" => 42,
                                                 "string" => "hey",
                                                 "u64" => 25,
                                                 "u64 array" => [12, 10, 13]},
                                                {"bool" => true,
                                                 "bool array" => [true],
                                                 "byte array" => [12, 10, 13].pack("C*"),
                                                 "f64:2" => 0.78,
                                                 "i64" => 42,
                                                 "string" => "hey",
                                                 "u64" => 25,
                                                 "u64 array" => [12, 10, 13]},
                                                {"bool" => true,
                                                 "bool array" => [true],
                                                 "byte array" => [12, 10, 13].pack("C*"),
                                                 "f64:2" => 0.78,
                                                 "i64" => 42,
                                                 "string" => "hey",
                                                 "u64" => 25,
                                                 "u64 array" => [12, 10, 13]}])

    expect(track_reader.section_encoding(1)).to eq(:standard)
    expect(track_reader.section_schema(1)).to eq([["a", :i64], ["b", :bool], ["c", :string]])
    expect(track_reader.section_rows(1)).to eq(3)
    expect(track_reader.section_data(1)).to eq([{"a"=>1, "b"=>false, "c"=>"Ride"},
                                                {"a"=>2, "c"=>"with"},
                                                {"a"=>4, "b"=>true, "c"=>"GPS"}])
  end

  it "raises an exception for an invalid section index" do
    track_reader = Tracklib::TrackReader.new(data)
    expect { track_reader.section_encoding(2) }.to raise_error("Section does not exist")
  end

  it "can select a subset of fields" do
    track_reader = Tracklib::TrackReader.new(data)
    # field that doesn't exist
    expect(track_reader.section_column(0, "missing column")).to eq(nil)

    # field that does exist
    expect(track_reader.section_column(0, "i64")).to eq([42, 42, 42, 42, 42])

    # missing values are nil
    expect(track_reader.section_column(1, "b")).to eq([false, nil, true])

    # invalid field type
    expect { track_reader.section_column(0, :foo) }.to raise_error("Error converting to String")

    # section doesn't exist
    expect { track_reader.section_column(2, "a") }.to raise_error("Section does not exist")
  end
end
