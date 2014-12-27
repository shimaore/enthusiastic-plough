What is it?
===========

A script to split the CDR database into smaller databases

How does it work?
=================

    run = (since,year) ->

It queries the CDR database in batches, and distributes each item into a target bin (database) for a given year.

      console.log "#{pkg.name} #{pkg.version} starting for year #{year} at sequence #{since}."

      limit = cfg.limit ? 500

      request
      .getAsync
        url: "#{cfg.source}/_changes"
        json: true
        qs:
          limit: limit
          since: since
          include_docs: true
      .then ({results}) ->
        savers = {}
        for change in results
          do (change) ->
            {seq,doc} = change
            target_month = doc?.variables?.start_stamp?.substr(0,7)
            if target_month? and target_month.substr(0,4) is year
              target = savers[target_month] ?= new Saver target_month
              target.push doc, seq
            else
              console.log "Skipped #{doc._id}"

        Promise.all savers.map (s) -> s.flush()
      .then ->
        seq

    pkg = require './package.json'
    cfg = require './config.json'
    Promise = require 'bluebird'
    PouchDB = require 'pouchdb'
    request = Promise.promisifyAll (require 'request').defaults cfg.ajax

    class SaverError extends Error

    class Saver
      constructor: (@name) ->
        @db = new PouchDB "#{cfg.targets}/cdrs-#{@name}", ajax: cfg.ajax
        @queue = []

      push: (doc) ->
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
    while true
      run since, year
      .then (seq) ->
        since = seq
