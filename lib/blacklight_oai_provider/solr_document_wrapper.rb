module BlacklightOaiProvider
  class SolrDocumentWrapper < ::OAI::Provider::Model
    attr_reader :model, :timestamp_field
    attr_accessor :options
    def initialize(controller, options = {})
      @controller = controller

      defaults = { :timestamp => 'timestamp', :limit => 15}
      @options = defaults.merge options

      @timestamp_field = @options[:timestamp]
      @limit = @options[:limit]
      puts @options
    end

    def sets
    end

    def earliest
      Time.parse @controller.get_search_results(@controller.params, {:fl => @timestamp_field, :sort => @timestamp_field +' asc', :rows => 1}).last.first.get(@timestamp_field)
    end

    def latest
      Time.parse @controller.get_search_results(@controller.params, {:fl => @timestamp_field, :sort => @timestamp_field +' desc', :rows => 1}).last.first.get(@timestamp_field)
    end

    def find(selector, options={})
      return next_set(options[:resumption_token]) if options[:resumption_token]

      if :all == selector
        if @controller.params.has_key?(:from) || @controller.params.has_key?(:until)
          @controller.params[:from] = parse_to_local(@controller.params[:from]) if @controller.params.has_key?(:from)
          @controller.params[:until] = parse_to_local(@controller.params[:until]) if @controller.params.has_key?(:until)
          @controller.params[:sort] = @timestamp_field + ' asc'
          @controller.params[:rows] = @limit
          @controller.solr_search_params_logic << :apply_oai_filters
          response, records = @controller.get_search_results
        else
          response, records = @controller.get_search_results(@controller.params, {:sort => @timestamp_field + ' asc', :rows => @limit})
        end

        if @limit && response.total >= @limit
          return select_partial(OAI::Provider::ResumptionToken.new(options.merge({:last => 0})))
        end
      else
        response, records = @controller.get_solr_response_for_doc_id selector.split('/', 2).last
      end
      records
    end

    def select_partial token
      if @controller.params.has_key?(:from) || @controller.params.has_key?(:until)
        @controller.params[:from] = parse_to_local(@controller.params[:from]) if @controller.params.has_key?(:from)
        @controller.params[:until] = parse_to_local(@controller.params[:until]) if @controller.params.has_key?(:until)
        @controller.params[:sort] = @timestamp_field + ' asc'
        @controller.params[:rows] = @limit
        @controller.solr_search_params_logic << :apply_oai_filters
        records = @controller.get_search_results(@controller.params, {:start => token.last}).last
      else
        records = @controller.get_search_results(@controller.params, {:sort => @timestamp_field + ' asc', :rows => @limit, :start => token.last}).last
      end
      raise ::OAI::ResumptionTokenException.new unless records

      OAI::Provider::PartialResult.new(records, token.next(token.last+@limit))
    end

    def next_set(token_string)
      raise ::OAI::ResumptionTokenException.new unless @limit

      token = OAI::Provider::ResumptionToken.parse(token_string)
      select_partial(token)
    end

    def parse_to_local(time)
      time_obj = Time.parse(time.to_s)
      # return time_obj.utc.xmlschema
      time_obj = yield(time_obj) if block_given?
      # Convert to same as DB - :local => :getlocal, :utc => :getutc
      tzconv = "get#{model.default_timezone.to_s}".to_sym
      time_obj.send(tzconv).strftime("%Y-%m-%d %H:%M:%S")
    end
  end
end
