require "spec_helper"

CONFIG = {"track_points"=>{"LongFloat"=>["x", "y", "e"],
                           "Number"=>["t"]},
          "course_points"=>{}}

describe RWTFile do
  context "round trips" do
    it "and drops empty keys" do
      orig_data = {"track_points"=>[{""=>3,
                                     "t"=>4,
                                     "x"=>4.4,
                                     "y"=>52.1,
                                     "e"=>-22.7}]}
      rwtf = RWTFile::from_h(orig_data, CONFIG)
      bytes = rwtf.to_bytes
      new_rwtf = RWTFile::from_bytes(bytes)
      new_data = new_rwtf.to_h

      expect(new_data).to eq({"track_points"=>[{"t"=>4,
                                                "x"=>4.4,
                                                "y"=>52.1,
                                                "e"=>-22.7}]})
    end
  end
end
