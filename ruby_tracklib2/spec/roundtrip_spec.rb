require "spec_helper"

describe Tracklib do
  it "can roundtrip an I64 column" do
    data = [{"a" => 0},
            {},
            {},
            {"a" => 40},
            {"a" => -40.0}]

    schema = Tracklib::Schema.new([["a", :i64]])
    section = Tracklib::Section::standard(schema, data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end

  it "can roundtrip an F64 column" do
    data = [{"a" => 0},
            {},
            {"a" => 11.2},
            {"a" => -400.000003}]

    schema = Tracklib::Schema.new([["a", :f64, 7]])
    section = Tracklib::Section::standard(schema, data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end

  it "can roundtrip an U64 column" do
    data = [{"a" => 0},
            {},
            {"a" => 11},
            {"a" => 400},
            {"a" => 20}]

    schema = Tracklib::Schema.new([["a", :u64]])
    section = Tracklib::Section::standard(schema, data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end

  it "can roundtrip a Bool column" do
    data = [{"a" => false},
            {},
            {"a" => true},
            {"a" => true},
            {}]

    schema = Tracklib::Schema.new([["a", :bool]])
    section = Tracklib::Section::standard(schema, data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end

  it "can roundtrip a String column" do
    data = [{"a" => "RWGPS"},
            {},
            {"a" => "Supercalifragilisticexpialidocious"},
            {"a" => ""}]

    schema = Tracklib::Schema.new([["a", :string]])
    section = Tracklib::Section::standard(schema, data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end

  it "can roundtrip a BoolArray column" do
    data = [{"a" => [true, false]},
            {},
            {"a" => [false, false, false, false, false, false, false, false, false, false, false]},
            {"a" => []}]

    schema = Tracklib::Schema.new([["a", :bool_array]])
    section = Tracklib::Section::standard(schema, data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end

  it "can roundtrip a U64Array column" do
    data = [{"a" => [0, 20, 1, 5_000]},
            {},
            {"a" => []},
            {"a" => [80_000_000, 5]}]

    schema = Tracklib::Schema.new([["a", :u64_array]])
    section = Tracklib::Section::standard(schema, data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end

  it "can roundtrip a ByteArray column" do
    example_string = [0, 65].pack("C*")
    expect(example_string.encoding()). to eq(Encoding::find("ASCII-8BIT"))
    data = [{"a" => "RWGPS"},
            {},
            {"a" => ""},
            {"a" => example_string}]

    schema = Tracklib::Schema.new([["a", :byte_array]])
    section = Tracklib::Section::standard(schema, data)
    buf = Tracklib::write_track([], [section])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.section_data(0)).to eq(data)
  end

  it "can roundtrip all types and metadata" do
    metadata = [[:created_at, Time.new(1970, 1, 2, 11, 12, 13, "UTC")],
                [:track_type, :route, 1000]]

    schema = [["i64", :i64],
              ["f64", :f64, 2],
              ["u64", :u64],
              ["bool", :bool],
              ["string", :string],
              ["boolarray", :bool_array],
              ["u64array", :u64_array],
              ["bytearray", :byte_array]]

    data0 = [{"i64" => -200,
              "f64" => 37.89,
              "u64" => 80_000_000_000,
              "bool" => true,
              "string" => "RWGPS",
              "boolarray" => [false, true, false],
              "u64array" => [20, 10, 11],
              "bytearray" => "RWGPS"}]
    section0 = Tracklib::Section::standard(Tracklib::Schema.new(schema), data0)

    data1 = [{"i64" => 11,
              "bool" => false,
              "string" => "Hello"},
             {"f64" => 21.12,
              "u64" => 2000,
              "boolarray" => [],
              "u64array" => [],
              "bytearray" => ""}]
    section1 = Tracklib::Section::encrypted(Tracklib::Schema.new(schema), data1, "01234567890123456789012345678901")

    buf = Tracklib::write_track(metadata, [section0, section1])
    reader = Tracklib::TrackReader::new(buf)
    expect(reader.metadata()).to eq(metadata)

    # Check section 0 - standard encoding

    expect(reader.section_schema(0)).to eq(schema)
    expect(reader.section_rows(0)).to eq(1)
    expect(reader.section_encoding(0)).to eq(:standard)
    expect(reader.section_data(0)).to eq(data0)
    expect(reader.section_column(0, "i64")).to eq([-200])

    # the password argument is ignored for a Standard section
    expect(reader.section_data(0, "01234567890123456789012345678901")).to eq(data0)
    expect(reader.section_data(0, nil)).to eq(data0)
    expect(reader.section_data(0, "Invalid Password")).to eq(data0)
    expect(reader.section_column(0, "i64", "Invalid Password")).to eq([-200])


    # Check section 1 - encrypted encoding

    expect(reader.section_schema(1)).to eq(schema)
    expect(reader.section_rows(1)).to eq(2)
    expect(reader.section_encoding(1)).to eq(:encrypted)
    expect(reader.section_data(1, "01234567890123456789012345678901")).to eq(data1)
    expect(reader.section_column(1, "i64", "01234567890123456789012345678901")).to eq([11, nil])
    expect(reader.section_column(1, "f64", "01234567890123456789012345678901")).to eq([nil, 21.12])

    # only the right password works when it's encrypted
    expect { reader.section_data(1, nil) }.to raise_error
    expect { reader.section_data(1, "Invalid Password") }.to raise_error
    expect { reader.section_data(1, "00004567890123456789012345678901") }.to raise_error
    expect { reader.section_column(1, "i64", "00004567890123456789012345678901") }.to raise_error
  end

  it "will trim schema fields" do
    schema = Tracklib::Schema.new([["a", :string],
                                   ["b", :bool],
                                   ["c", :u64]])
    data = [{"a" => "RWGPS"},
            {},
            {"a" => "", "c" => 0}]
    key = "01234567890123456789012345678901"
    standard_section = Tracklib::Section::standard(schema, data)
    encrypted_section = Tracklib::Section::encrypted(schema, data, key)
    buf = Tracklib::write_track([], [standard_section, encrypted_section])
    reader = Tracklib::TrackReader::new(buf)

    expect(reader.section_data(0)).to eq(data)
    expect(reader.section_schema(0)).to eq([["a", :string], ["c", :u64]])

    expect(reader.section_data(1, key)).to eq(data)
    expect(reader.section_schema(1)).to eq([["a", :string], ["c", :u64]])
  end
end
