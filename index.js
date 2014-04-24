/* Copyright (c) 2014 Matteo Collina, ISC License
 *
 * Based on seneca-jsonfile-store
 * Copyright (c) 2013 Richard Rodger, MIT License */
"use strict";


var Sharder = require('sharder')
var async = require('async')
var name = 'shard-store'
var _ = require('underscore')


module.exports = function(seneca, opts, cb) {

  var shards = new Sharder(opts)

  for(var shardId in opts.shards) {
    var shard = opts.shards[shardId]
    var fixArgs = { shard: shard.zone /*, base: shard.base, name: shard.name*/ }
    console.log(JSON.stringify(fixArgs), shard.store.plugin, JSON.stringify(shard.store.options))
    seneca.fix( fixArgs ).use( shard.store.plugin, shard.store.options);
  }

  function act(args, shard, cb, skipError) {
    var toact = _.clone(args)

//     if (shard.name)
//       toact.name = shard.name

//     if (shard.base)
//       toact.base = shard.base

    if (shard.zone)
      toact.shard = shard.zone

    console.log(JSON.stringify(toact))

    this.act(toact, function(err, result) {
      cb(!skipError && err, result)
    })
  }

  function shardWrap(args, cb) {
    var seneca = this

    var id
      , shard

    if (args.ent) {
      id = args.ent.id
    } else if (args.q) {
      id = args.q.id
    }

    if (args.cmd !== 'save' && !id) {
      // shardWrapAll.call here is just to be clean and execute wrapAll in the right seneca context
      return shardWrapAll.call(seneca, args, function(err, list) {
        cb(err, list && list[0])
      })
    }

    if (args.cmd === 'save' && !id) {
      id = args.ent.id || shards.generate()
      args.ent.id$ = id
    }

    shard = shards.resolve(id)

    act.call(seneca, args, shard, cb)
  }

  function shardWrapAll(args, cb) {
    var seneca = this
    // TODO should we handle reordering of results?
    async.concat(Object.keys(shards.shards), function(shard, cb) {
      act.call(seneca, args, shards.shards[shard], cb, true)
    }, cb)
  }

  var store = {
    name: name,

    save: shardWrap,

    load: shardWrap,

    list: shardWrapAll,

    remove: shardWrapAll,

    close: shardWrapAll,

    native: function(done){
      done(null,opts)
    }
  }


  seneca.store.init(seneca,opts,store,function(err,tag,description){
    if( err ) return cb(err);

    cb(null,{name:store.name,tag:tag})
  })
}


