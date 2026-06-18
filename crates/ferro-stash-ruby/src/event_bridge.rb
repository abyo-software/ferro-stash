# frozen_string_literal: true
#
# LogStash::Event compatible class for FerroStash Ruby filter.
#
# This pure-Ruby implementation wraps the native event data passed from Rust.
# The Rust side injects event data as a global Hash ($__ferro_event_data__)
# and reads it back after execution.

module LogStash
  class Event
    TIMESTAMP = '@timestamp'.freeze
    METADATA = '@metadata'.freeze
    METADATA_BRACKETS = '[@metadata]'.freeze

    def initialize(data = nil)
      if data.nil?
        @data = {}
        @metadata = {}
        @tags = []
        @cancelled = false
        @timestamp = Time.now.strftime('%Y-%m-%dT%H:%M:%S.%LZ') rescue Time.now.to_s
      else
        @data = data.is_a?(Hash) ? data.dup : {}
        @metadata = (@data.delete('@metadata') || {}).dup
        @tags = Array(@data.delete('tags') || [])
        @cancelled = false
        @timestamp = @data.delete('@timestamp') || (Time.now.strftime('%Y-%m-%dT%H:%M:%S.%LZ') rescue Time.now.to_s)
      end
    end

    # Field reference parser: "[field][subfield]" or "field"
    def self.parse_field_ref(field)
      return [field] unless field.include?('[')
      parts = []
      field.scan(/\[([^\]]*)\]/) { |m| parts << m[0] }
      parts = [field] if parts.empty?
      parts
    end

    def get(field)
      return @timestamp if field == TIMESTAMP

      if field.start_with?('[@metadata]')
        meta_field = field.sub('[@metadata]', '').gsub(/^\[/, '').gsub(/\]$/, '')
        if meta_field.empty?
          return @metadata
        end
        parts = self.class.parse_field_ref("[#{meta_field}]")
        return dig_hash(@metadata, parts)
      end

      if field == 'tags'
        return @tags
      end

      parts = self.class.parse_field_ref(field)
      dig_hash(@data, parts)
    end

    def set(field, value)
      if field == TIMESTAMP
        @timestamp = value
        return value
      end

      if field.start_with?('[@metadata]')
        meta_field = field.sub('[@metadata]', '').gsub(/^\[/, '').gsub(/\]$/, '')
        if meta_field.empty?
          @metadata = value.is_a?(Hash) ? value : {}
          return value
        end
        parts = self.class.parse_field_ref("[#{meta_field}]")
        set_hash(@metadata, parts, value)
        return value
      end

      if field == 'tags'
        @tags = Array(value)
        return value
      end

      parts = self.class.parse_field_ref(field)
      set_hash(@data, parts, value)
      value
    end

    def remove(field)
      if field == TIMESTAMP
        old = @timestamp
        @timestamp = nil
        return old
      end

      if field.start_with?('[@metadata]')
        meta_field = field.sub('[@metadata]', '').gsub(/^\[/, '').gsub(/\]$/, '')
        if meta_field.empty?
          old = @metadata
          @metadata = {}
          return old
        end
        parts = self.class.parse_field_ref("[#{meta_field}]")
        return remove_hash(@metadata, parts)
      end

      if field == 'tags'
        old = @tags
        @tags = []
        return old
      end

      parts = self.class.parse_field_ref(field)
      remove_hash(@data, parts)
    end

    def include?(field)
      !get(field).nil?
    end

    def tag(value)
      @tags << value unless @tags.include?(value)
    end

    def cancel
      @cancelled = true
    end

    def uncancel
      @cancelled = false
    end

    def cancelled?
      @cancelled
    end

    def timestamp
      @timestamp
    end

    def to_hash
      result = @data.dup
      result['@timestamp'] = @timestamp if @timestamp
      result['tags'] = @tags.dup unless @tags.empty?
      result
    end

    def to_hash_with_metadata
      result = to_hash
      result['@metadata'] = @metadata.dup unless @metadata.empty?
      result
    end

    def to_json(*args)
      require 'json'
      to_hash.to_json(*args)
    end

    def to_json_with_metadata(*args)
      require 'json'
      to_hash_with_metadata.to_json(*args)
    end

    def to_s
      to_hash.inspect
    end

    def inspect
      "#<LogStash::Event #{to_hash.inspect}>"
    end

    def clone
      cloned = self.class.new(to_hash_with_metadata)
      cloned.instance_variable_set(:@timestamp, @timestamp)
      cloned.instance_variable_set(:@cancelled, false)
      cloned
    end

    def sprintf(format)
      result = format.gsub(/%\{([^}]+)\}/) do |_match|
        field = $1
        val = get(field)
        val.nil? ? "%{#{field}}" : val.to_s
      end
      result
    end

    # Serialize back to the format Rust expects
    def __to_ferro_hash__
      h = to_hash_with_metadata
      h['__cancelled__'] = @cancelled
      h
    end

    private

    def dig_hash(hash, parts)
      current = hash
      parts.each do |part|
        if current.is_a?(Hash)
          current = current[part]
        elsif current.is_a?(Array) && part =~ /\A\d+\z/
          current = current[part.to_i]
        else
          return nil
        end
      end
      current
    end

    def set_hash(hash, parts, value)
      if parts.length == 1
        hash[parts[0]] = value
        return
      end

      current = hash
      parts[0...-1].each do |part|
        current[part] = {} unless current[part].is_a?(Hash)
        current = current[part]
      end
      current[parts[-1]] = value
    end

    def remove_hash(hash, parts)
      if parts.length == 1
        return hash.delete(parts[0])
      end

      current = hash
      parts[0...-1].each do |part|
        current = current[part]
        return nil unless current.is_a?(Hash)
      end
      current.delete(parts[-1])
    end
  end
end
