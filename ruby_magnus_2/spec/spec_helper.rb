# frozen_string_literal: true

require "ruby_tracklib"

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end

def decode_polyline(polyline, precisions) # rubocop:disable Metrics/AbcSize
  points = []
  index = 0
  prevs = precisions.collect { |_| 0.0 }

  while index < polyline.length
    precisions.each_with_index do |precision, precision_index|
      result = 1
      shift = 0
      loop do
        b = polyline[index].ord - 63 - 1
        index += 1
        result += b << shift
        shift += 5
        break if b < 0x1f
      end
      prevs[precision_index] += (result & 1) == 0 ? (result >> 1) : (~result >> 1)

      points << (prevs[precision_index] / (10**precision))
    end

  end

  points
end
