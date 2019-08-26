require "spec_helper"

CONFIG = {"track_points"=>{"LongFloat"=>["x", "y", "e"],
                           "ShortFloat"=>["s", "d"],
                           "Number"=>["t", "c", "h"],
                           "Base64"=>["ep"]},
          "course_points"=>{}}

def roundtrip(data, field_config)
  rwtf = RWTFile::from_h(data, field_config)
  bytes = rwtf.to_bytes
  new_rwtf = RWTFile::from_bytes(bytes)
  return new_rwtf.to_h
end

describe RWTFile do
  context "round trips" do
    it "and drops empty keys" do
      orig_data = {"track_points"=>[{""=>3,
                                     "t"=>4,
                                     "x"=>4.4,
                                     "y"=>52.1,
                                     "e"=>-22.7}]}
      expect(roundtrip(orig_data, CONFIG)).to eq({"track_points"=>[{"t"=>4,
                                                                    "x"=>4.4,
                                                                    "y"=>52.1,
                                                                    "e"=>-22.7}]})
    end
    it "and doesn't drop a point" do
      orig_data = {"track_points"=>[{"t"=>1,
                                     "e"=>40},
                                    {},
                                    {"t"=>nil},
                                    {"t"=>4}]}
      expect(roundtrip(orig_data, CONFIG)).to eq({"track_points"=>[{"t"=>1,
                                                                    "e"=>40},
                                                                   {},
                                                                   {},
                                                                   {"t"=>4}]})
    end
    it "and drops numbers that are too large to turn into primitives" do
      # integer
      orig_data = {"track_points"=>[{"x"=>10000000000000000000000000000000000, "y"=>7.2}]}
      expect(roundtrip(orig_data, CONFIG)).to eq({"track_points"=>[{"y"=>7.2}]})

      # float
      orig_data = {"track_points"=>[{"x"=>10000000000000000000000000000000000.3, "y"=>7.2}]}
      expect(roundtrip(orig_data, CONFIG)).to eq({"track_points"=>[{"y"=>7.2}]})
    end
    it "and drops numbers that are valid primitives but still too large for tracklib" do
      ## Numbers
      # drops 2**60
      orig_data = {"track_points"=>[{"t"=>2**60, "y"=>7.2}]}
      expect(roundtrip(orig_data, CONFIG)).to eq({"track_points"=>[{"y"=>7.2}]})
      # allows 2**48-1
      orig_data = {"track_points"=>[{"t"=>2**48-1, "y"=>7.2}]}
      expect(roundtrip(orig_data, CONFIG)).to eq(orig_data)
      # allows floats even though they should be ints
      orig_data = {"track_points"=>[{"t"=>2.0**48-1.0, "y"=>7.2}]}
      expect(roundtrip(orig_data, CONFIG)).to eq(orig_data)

      ## LongFloat
      # drops 2.0**48-1
      orig_data = {"track_points"=>[{"x"=>2.0**48-1.0, "y"=>7.2}]}
      expect(roundtrip(orig_data, CONFIG)).to eq({"track_points"=>[{"y"=>7.2}]})
      # allows 2.0**24-1
      orig_data = {"track_points"=>[{"x"=>2.0**24-1.0, "y"=>7.2}]}
      expect(roundtrip(orig_data, CONFIG)).to eq(orig_data)
      # allows ints even though they should be floats
      orig_data = {"track_points"=>[{"x"=>2**24-1, "y"=>7.2}]}
      expect(roundtrip(orig_data, CONFIG)).to eq(orig_data)

      ## ShortFloat
      # drops 2.0**48-1
      orig_data = {"track_points"=>[{"s"=>2.0**48-1.0, "y"=>7.2}]}
      expect(roundtrip(orig_data, CONFIG)).to eq({"track_points"=>[{"y"=>7.2}]})
      # allows 2.0**38-1
      orig_data = {"track_points"=>[{"s"=>2.0**38-1.0, "y"=>7.2}]}
      expect(roundtrip(orig_data, CONFIG)).to eq(orig_data)
      # allows ints even though they should be floats
      orig_data = {"track_points"=>[{"s"=>2**38-1, "y"=>7.2}]}
      expect(roundtrip(orig_data, CONFIG)).to eq(orig_data)
    end
  end
end
