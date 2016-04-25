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
    end

    def sets
      set_list = []

      etds = OAI::Set.new()
      etds.name = "Theses and Dissertations"
      etds.spec = "00000000"
      set_list << etds

      research = OAI::Set.new()
      research.name = "Research Publications"
      research.spec = "00000001"
      set_list << research

      presentations = OAI::Set.new()
      presentations.name = "Presentations"
      presentations.spec = "00000002"
      set_list << presentations

      monographs = OAI::Set.new()
      monographs.name = "Monographs"
      monographs.spec = "00000003"
      set_list << monographs

      technical_reports = OAI::Set.new()
      technical_reports.name = "Technical Reports"
      technical_reports.spec = "00000004"
      set_list << technical_reports

      query_result = ActiveFedora::SolrService.query("published_set_tesim:\"true\"")

      if query_result.count != 0
        query_result.each do |qr|
          doc = SolrDocument.new(qr)
          tmp_set = OAI::Set.new()
          tmp_set.name = doc.title
          tmp_set.spec = doc.pid.split(":").last
          set_list << tmp_set
        end
      end

      return set_list
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
          @controller.params[:from] = parse_time(@controller.params[:from]) if @controller.params.has_key?(:from)
          @controller.params[:until] = parse_time(@controller.params[:until], true) if @controller.params.has_key?(:until)
          @controller.solr_search_params_logic << :oai_time_filters
        end
        if @controller.params.has_key?(:set)
          if @controller.params[:set] == "00000000"
            @controller.solr_search_params_logic << :theses_and_dissertations_filter
          elsif @controller.params[:set] == "00000001"
            @controller.solr_search_params_logic << :research_filter
          elsif @controller.params[:set] == "00000002"
            @controller.solr_search_params_logic << :presentations_filter
          elsif @controller.params[:set] == "00000003"
            @controller.solr_search_params_logic << :monographs_filter
          elsif @controller.params[:set] == "00000004"
            @controller.solr_search_params_logic << :technical_reports_filter
          else
            @controller.solr_search_params_logic << :oai_set_filter
          end
        end

        response, records = @controller.get_search_results(@controller.params, {:sort => @timestamp_field + ' asc', :rows => @limit})

        if @limit && response.total >= @limit
          return select_partial(OAI::Provider::ResumptionToken.new(options.merge({:last => 0})))
        end
      else
        response, records = @controller.get_solr_response_for_doc_id selector.split('/', 2).last
      end
      records
    end

    def select_partial token
      if !token.from.blank? || !token.until.blank?
        @controller.params[:from] = parse_time(token.from) if !token.from.blank?
        @controller.params[:until] = parse_time(token.until) if !token.until.blank?
        @controller.solr_search_params_logic << :oai_time_filters
      end
      if !token.set.blank?
        if token.set == "00000000"
          @controller.solr_search_params_logic << :theses_and_dissertations_filter
        elsif token.set == "00000001"
          @controller.solr_search_params_logic << :research_filter
        elsif token.set == "00000002"
          @controller.solr_search_params_logic << :presentations_filter
        elsif token.set == "00000003"
          @controller.solr_search_params_logic << :monographs_filter
        elsif token.set == "00000004"
          @controller.solr_search_params_logic << :technical_reports_filter
        else
          @controller.solr_search_params_logic << :oai_set_filter
        end
      end

      records = @controller.get_search_results(@controller.params, {:sort => @timestamp_field + ' asc', :rows => @limit, :start => token.last}).last
      raise ::OAI::ResumptionTokenException.new unless records

      OAI::Provider::PartialResult.new(records, token.next(token.last+@limit))
    end

    def next_set(token_string)
      raise ::OAI::ResumptionTokenException.new unless @limit

      token = OAI::Provider::ResumptionToken.parse(token_string)
      select_partial(token)
    end

    def parse_time(time, bump=false)
      time_obj = Time.parse(time.to_s)
      if bump #the oai spec says that until must allow for the until date to be included, so we will bump up the time by 1 second
        time_obj = time_obj.+1.second
      end
      return time_obj.utc.xmlschema
    end
  end
end
