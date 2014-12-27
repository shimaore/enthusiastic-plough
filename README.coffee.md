What is it?
===========

A script to split the CDR database into smaller databases

How does it work?
=================

    run = (since,year) ->
      assert since?, 'since is required'
      assert year?, 'year is required'

It queries the CDR database in batches, and distributes each item into a target bin (database) for a given year.

      limit = cfg.limit ? 500

      console.log "#{pkg.name} #{pkg.version} starting for year #{year} at sequence #{since} for up to #{limit}."

      options =
        url: "#{cfg.source}/_changes"
        json: true
        qs:
          limit: limit
          since: since
          include_docs: true
      for k,v of cfg.ajax
        options[k] = v
      console.log "options = #{JSON.stringify options}"
      request.getAsync options
      .catch (error) ->
        console.log "getAsync failed with #{error}"
        throw error
      .then ({results}) ->
        assert results?, 'Missing results.'
        assert results.length > 0, 'No results.'
        console.log "Splitting #{results.length} results."
        savers = {}
        for change in results
          do (change) ->
            {seq,doc} = change
            since = seq
            assert seq?, 'Missing seq'
            assert doc?, 'Missing doc'
            target_month = doc?.variables?.start_stamp?.substr(0,7)
            if target_month? and target_month.substr(0,4) is year
              target = savers[target_month] ?= new Saver target_month
              target.push doc
            else
              console.log "Skipped #{doc._id}"

        observers = []
        for name,saver of savers
          do (saver) ->
            observers.push saver.flush()

        Promise.all observers
      .then ->
        run since, year

    pkg = require './package.json'
    cfg = require './config.json'
    Promise = require 'bluebird'
    PouchDB = require 'pouchdb'
    request = Promise.promisifyAll (require 'request').defaults cfg.ajax
    assert = require 'assert'

    class SaverError extends Error

    class Saver
      constructor: (@name) ->
        assert @name?, 'Missing @name'
        @db = new PouchDB "#{cfg.targets}/cdrs-#{@name}", ajax: cfg.ajax
        @queue = []

      push: (doc) ->
        assert doc?, 'Missing doc'
        delete doc._rev
        v = doc.variables
        doc._id = "#{v.start_stamp} #{v.ccnq_account} #{v.ccnq_from_e164} #{v.ccnq_to_e164} #{v.billsec}"
        @queue.push doc
        return

      flush: ->
        my_queue = @queue
        delete @queue
        console.log "Submitting #{my_queue.length} entries."
        @db.bulkDocs my_queue
        .then (responses) =>
          for response in responses when not response.ok
            throw new SaverError "Failed for #{response}"

    since = cfg.since
    year = cfg.year
    run since, year
    .catch (error) ->
      console.log "Stopped with #{error}"
      throw error
