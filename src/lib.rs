use std::ops::Range;

use ds_libs::{
    address::Address, model_checking::StateTestHarness, Application, Context, HandleMessage,
    HandleTimer,
};

use system::{MsgWithDst, State};

// **********************************************************************
//                      User items
// **********************************************************************

mod user {
    use std::{fmt::Debug, hash::Hash, time::Duration};

    use ds_libs::{
        address::{Address, NodeType},
        amo_application::{AMOApplication, Request, Response},
        Application, Context, HandleMessage, HandleTimer, InitializeNode,
    };

    use crate::system::InnerCtx;

    use derivative::Derivative;

    #[derive(Debug, Hash, PartialEq, Eq, Clone, PartialOrd, Ord)]
    pub struct ResendTimer(usize);

    #[derive(Derivative)]
    #[derivative(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
    #[derivative(PartialEq(bound = "App::Command: PartialEq, App::Res: PartialEq"))]
    #[derivative(PartialOrd(bound = "App::Command: PartialOrd, App::Res: PartialOrd"))]
    #[derivative(Eq(bound = "App::Command: Eq, App::Res: Eq"))]
    #[derivative(Ord(bound = "App::Command: Ord, App::Res: Ord"))]
    #[derivative(Debug(bound = "App::Command: Debug, App::Res: Debug"))]
    #[derivative(Clone(bound = "App::Command: Clone, App::Res: Clone"))]
    #[derivative(Hash(bound = "App::Command: Hash, App::Res: Hash"))]
    pub struct Client<App>
    where
        App: Application,
    {
        pub sequence_number: usize,
        pub command: Option<App::Command>,
        pub server_address: Address<Server<App>>,
        pub response: Option<App::Res>,
    }

    #[derive(Derivative)]
    #[derivative(PartialEq, Eq, PartialOrd, Ord, Debug, Clone)]
    #[derivative(PartialEq(
        bound = "App::Command: PartialEq, App::Res: PartialEq, App: PartialEq"
    ))]
    #[derivative(PartialOrd(
        bound = "App::Command: PartialOrd, App::Res: PartialOrd, App: PartialOrd"
    ))]
    #[derivative(Eq(bound = "App::Command: Eq, App::Res: Eq, App: Eq"))]
    #[derivative(Ord(bound = "App::Command: Ord, App::Res: Ord, App: Ord"))]
    #[derivative(Debug(bound = "App::Command: Debug, App::Res: Debug, App: Debug"))]
    #[derivative(Clone(bound = "App::Command: Clone, App::Res: Clone, App: Clone"))]
    #[derivative(Hash(bound = "App::Command: Hash, App::Res: Hash, App: Hash"))]
    pub struct Server<App>
    where
        App: Application,
    {
        pub app: AMOApplication<App>,
    }

    // ----------------------------------------------------------------------

    impl<App> Server<App>
    where
        App: Application,
    {
        #[allow(unused)]
        pub fn new(app: App) -> Server<App> {
            Server {
                app: AMOApplication::new(app),
            }
        }
    }

    impl<App> Client<App>
    where
        App: Application,
    {
        #[allow(unused)]
        pub fn new(server: Address<Server<App>>, command: Option<App::Command>) -> Self {
            Client {
                sequence_number: 1,
                command,
                server_address: server,
                response: None,
            }
        }

        pub fn send_command(&self, ctx: &mut Context<Self, InnerCtx<App>>)
        where
            App::Command: Ord + Clone,
            App::Res: Ord + Clone,
        {
            if let Some(ref command) = self.command {
                let req = Request {
                    sequence_number: self.sequence_number,
                    cmd: command.clone(),
                    sender: ctx.my_address(),
                };

                ctx.send(self.server_address, req);
                ctx.set(
                    ResendTimer(self.sequence_number),
                    Duration::from_millis(100),
                );
            }
        }
    }

    impl<App> NodeType for Server<App>
    where
        App: Application,
    {
        fn node_type() -> &'static str {
            "server"
        }
    }

    impl<App> NodeType for Client<App>
    where
        App: Application,
    {
        fn node_type() -> &'static str {
            "client"
        }
    }

    impl<App> InitializeNode<InnerCtx<App>> for Server<App>
    where
        App: Application,
    {
        fn init(&mut self, _ctx: &mut Context<Self, InnerCtx<App>>) {}
    }

    impl<App> InitializeNode<InnerCtx<App>> for Client<App>
    where
        App: Application,
        App::Command: Ord + Clone,
        App::Res: Ord + Clone,
    {
        fn init(&mut self, ctx: &mut Context<Self, InnerCtx<App>>) {
            self.send_command(ctx);
        }
    }

    impl<App> HandleMessage<Request<App::Command, Client<App>>, InnerCtx<App>> for Server<App>
    where
        App: Application,
        App::Res: Clone + Ord,
    {
        fn handle_message(
            &mut self,
            ctx: &mut Context<Self, InnerCtx<App>>,
            req: Request<App::Command, Client<App>>,
        ) {
            let sender = req.sender;
            if let Some(res) = self.app.process(req) {
                ctx.send(sender, res);
            }
        }
    }

    impl<App> HandleMessage<Response<App::Res>, InnerCtx<App>> for Client<App>
    where
        App: Application,
    {
        fn handle_message(
            &mut self,
            _ctx: &mut Context<Self, InnerCtx<App>>,
            msg: Response<App::Res>,
        ) {
            if msg.sequence_number == self.sequence_number {
                self.command = None;
                self.sequence_number += 1;
                self.response = Some(msg.result);
            }
        }
    }

    impl<App> HandleTimer<ResendTimer, InnerCtx<App>> for Client<App>
    where
        App: Application,
        App::Command: Ord + Clone,
        App::Res: Ord + Clone,
    {
        fn handle_timer(&mut self, ctx: &mut Context<Self, InnerCtx<App>>, timer: ResendTimer) {
            if self.sequence_number == timer.0 {
                self.send_command(ctx);
            }
        }
    }
}

// **********************************************************************
//                          System
// **********************************************************************

mod system {
    use std::{collections::BTreeMap, fmt::Debug, hash::Hash};

    use derivative::Derivative;
    use ds_libs::{
        address::Address,
        amo_application::{testing::get_app, Request, Response},
        Application, Context, InitializeNode, ManageMessageType, ManageNodeType, ManageTimerType,
    };
    use hi_set::HISet;

    use crate::user::{Client, ResendTimer, Server};

    #[derive(Derivative)]
    #[derivative(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Hash)]
    #[derivative(PartialEq(
        bound = "App::Command: PartialEq, App::Res: PartialEq, App: PartialEq"
    ))]
    #[derivative(PartialOrd(
        bound = "App::Command: PartialOrd, App::Res: PartialOrd, App: PartialOrd"
    ))]
    #[derivative(Eq(bound = "App::Command: Eq, App::Res: Eq, App: Eq"))]
    #[derivative(Ord(bound = "App::Command: Ord, App::Res: Ord, App: Ord"))]
    #[derivative(Debug(bound = "App::Command: Debug, App::Res: Debug, App: Debug"))]
    #[derivative(Clone(bound = "App::Command: Clone, App::Res: Clone, App: Clone"))]
    #[derivative(Hash(bound = "App::Command: Hash, App::Res: Hash, App: Hash"))]
    pub struct State<App>
    where
        App: Application,
    {
        pub servers: BTreeMap<usize, Server<App>>,
        pub clients: BTreeMap<usize, Client<App>>,
        pub ctx: InnerCtx<App>,
    }

    #[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
    pub struct InnerCtx<App>
    where
        App: Application,
    {
        pub responses: HISet<MsgWithDst<Response<App::Res>>>,
        pub requests: HISet<MsgWithDst<Request<App::Command, Client<App>>>>,
        pub resend_timers: HISet<MsgWithDst<ResendTimer>>,
    }

    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
    pub struct MsgWithDst<Msg> {
        pub dst: usize,
        pub msg: Msg,
    }

    // ----------------------------------------------------------------------

    impl<App> State<App>
    where
        App: Application,
        App::Command: Ord + Clone,
        App::Res: Ord + Clone,
    {
        #[allow(unused)]
        pub fn new() -> State<App> {
            State {
                servers: BTreeMap::new(),
                clients: BTreeMap::new(),
                ctx: InnerCtx {
                    responses: HISet::new(),
                    requests: HISet::new(),
                    resend_timers: HISet::new(),
                },
            }
        }

        #[allow(unused)]
        pub fn change_command(&mut self, address: Address<Client<App>>, cmd: Option<App::Command>) {
            let client = self.clients.get_mut(&address.id()).unwrap();
            client.command = cmd;
            client.send_command(&mut Context::new(address, &mut self.ctx));
        }

        #[allow(unused)]
        pub fn get_response(&self, client: Address<Client<App>>) -> Option<App::Res>
        where
            App::Res: Clone,
        {
            self.clients.get(&client.id()).unwrap().response.clone()
        }

        #[allow(unused)]
        pub fn clear_response(&mut self, client: Address<Client<App>>) {
            self.clients.get_mut(&client.id()).unwrap().response = None;
        }

        #[allow(unused)]
        pub fn get_app(&self) -> &App {
            assert!(
                self.servers.len() == 1,
                "Can not get the application if there is no server"
            );

            get_app(&self.servers.iter().next().unwrap().1.app)
        }
    }

    impl<App> ManageNodeType<Server<App>, InnerCtx<App>> for State<App>
    where
        App: Application,
    {
        fn add_node(&mut self, address: Address<Server<App>>, node: Server<App>) {
            assert!(
                self.servers.len() == 0,
                "Tried to add more than one server."
            );
            self.servers.insert(address.id(), node);
        }
    }

    impl<App> ManageNodeType<Client<App>, InnerCtx<App>> for State<App>
    where
        App: Application,
        App::Command: Ord + Clone,
        App::Res: Ord + Clone,
    {
        fn add_node(&mut self, address: Address<Client<App>>, mut node: Client<App>) {
            assert!(
                !self.clients.contains_key(&address.id()),
                "Attempted to add a Node to a State which already has a Node with the same ID"
            );

            node.init(&mut Context::new(address, &mut self.ctx));
            self.clients.insert(address.id(), node);
        }
    }

    impl<App> ManageMessageType<Response<App::Res>> for InnerCtx<App>
    where
        App: Application,
        App::Res: Ord,
    {
        fn add(&mut self, dst: usize, msg: Response<App::Res>) {
            self.responses.insert(MsgWithDst { dst, msg });
        }
    }

    impl<App> ManageMessageType<Request<App::Command, Client<App>>> for InnerCtx<App>
    where
        App: Application,
        App::Command: Ord,
    {
        fn add(&mut self, dst: usize, msg: Request<App::Command, Client<App>>) {
            self.requests.insert(MsgWithDst { dst, msg });
        }
    }

    impl<App> ManageTimerType<ResendTimer> for InnerCtx<App>
    where
        App: Application,
    {
        fn add(&mut self, node: usize, timer: ResendTimer, _length: std::time::Duration) {
            self.resend_timers.insert(MsgWithDst {
                dst: node,
                msg: timer,
            });
        }
    }
}

// ***************************************************************************
//                              Searcher
// ***************************************************************************

macro_rules! define_deliver_messages {
    (use $self:ident and $id:ident, [$($msg:ident to [ $($node:ident),+ ]),+] ) => {
        $(
            if $self.ctx.$msg.len() > $id {
                let MsgWithDst { dst, msg } = $self.ctx.$msg.remove_index($id);

                $(
                    if $self.$node.contains_key(&dst) {
                        let dst_node = $self.$node.get_mut(&dst).unwrap();
                        dst_node.handle_message(
                            &mut Context::new(unsafe { Address::new(dst) }, &mut $self.ctx),
                            msg,
                        );
                        return;
                    }
                )+

                unreachable!();
            }
            #[allow(unused)]
            let $id = $id - $self.ctx.requests.len();
        )+

        unreachable!();
    };
}

macro_rules! define_drop_messages {
    (use $self:ident and $id:ident, [$($msg:ident),+] ) => {
        $(
            if $self.ctx.$msg.len() > $id {
                $self.ctx.$msg.remove_index($id);
                return;
            }
            #[allow(unused)]
            let $id = $id - $self.ctx.requests.len();
        )+

        unreachable!();
    };
}

macro_rules! define_duplicate_messages {
    (use $self:ident and $id:ident, [$($msg:ident to [ $($node:ident),+ ]),+] ) => {
        $(
            if $self.ctx.$msg.len() > $id {
                let MsgWithDst { dst, msg } = $self.ctx.$msg.get_index($id).clone();

                $(
                    if $self.$node.contains_key(&dst) {
                        let dst_node = $self.$node.get_mut(&dst).unwrap();
                        dst_node.handle_message(
                            &mut Context::new(unsafe { Address::new(dst) }, &mut $self.ctx),
                            msg,
                        );
                        return;
                    }
                )+

                unreachable!();
            }
            #[allow(unused)]
            let $id = $id - $self.ctx.requests.len();
        )+

        unreachable!();
    };
}

macro_rules! define_get_ids {
    (use $self:ident, [$($entity:ident),+] ) => {
        return 0..(0$(
            + $self.ctx.$entity.len()
        )+);
    };
}

macro_rules! define_ring_timers {
    (use $self:ident and $id:ident, [$($timer:ident to [ $($node:ident),+ ]),+] ) => {
        $(
            if $self.ctx.$timer.len() > $id {
                let MsgWithDst {
                    dst: node,
                    msg: timer,
                } = $self.ctx.$timer.remove_index($id);

                $(
                    if $self.$node.contains_key(&node) {
                        $self.$node.get_mut(&node).unwrap().handle_timer(
                            &mut Context::new(unsafe { Address::new(node) }, &mut $self.ctx),
                            timer,
                        );
                        return;
                    }
                )+

                unreachable!();
            }

            #[allow(unused)]
            let $id = $id - $self.ctx.$timer.len();
        )+

        unreachable!();
    };
}

impl<App> StateTestHarness for State<App>
where
    App: Application,
    App::Command: Ord + Clone,
    App::Res: Ord + Clone,
{
    type MsgID = usize;

    type MsgIter = Range<usize>;

    fn deliver_message(&mut self, id: Self::MsgID) {
        define_deliver_messages!(use self and id, [requests to [servers], responses to [clients]]);
    }

    fn drop_message(&mut self, id: Self::MsgID) {
        define_drop_messages!(use self and id, [requests, responses]);
    }

    fn duplicate_and_deliver_message(&mut self, id: Self::MsgID) {
        define_duplicate_messages!(use self and id, [requests to [servers], responses to [clients]]);
    }

    fn get_message_ids(&self) -> Self::MsgIter {
        define_get_ids!(use self, [requests, responses]);
    }

    type TimerID = usize;
    type TimerIter = Range<usize>;

    fn get_timer_ids(&self) -> Self::TimerIter {
        define_get_ids!(use self, [resend_timers]);
    }

    fn ring_timer(&mut self, id: Self::TimerID) {
        define_ring_timers!(use self and id, [resend_timers to [clients]]);
    }
}

#[cfg(test)]
mod tests {

    use std::time::Duration;

    use ds_libs::{
        address::AddressConstructor, model_checking::real_world::UnstableNetworkWrapper,
        ManageNodeType,
    };
    use map_application::{Command, CommandResponse, MapApplication};
    use model_checking::{
        multi_phase_searcher::{begin_multi_phase_search, begin_multi_phase_search_with_name},
        wrappers::caches::bloom_cache::BloomCache,
        SearchConfig,
    };
    use user::{Client, Server};

    use super::*;

    #[test]
    fn two_servers_both_see_change() {
        // setup
        let mut addr_fact = AddressConstructor::new();

        let server = addr_fact.construct_address();
        let server_node = Server::new(MapApplication::new());

        let client1 = addr_fact.construct_address();
        let client1_node = Client::new(server, Some(Command::Store(255, 42)));
        let client2 = addr_fact.construct_address();
        let client2_node = Client::new(server, None);

        let mut start = State::new();
        start.add_node(server, server_node);
        start.add_node(client1, client1_node);
        start.add_node(client2, client2_node);

        let mut base_searcher =
            SearchConfig::new(BloomCache::new(UnstableNetworkWrapper::new(start)));
        base_searcher.set_timeout(Duration::from_secs(5));

        let res = begin_multi_phase_search(base_searcher)
            .search(|s| {
                s.get_response(client1).is_some()
                    && s.ctx.responses.len() == 0
                    && s.ctx.requests.len() == 0
            })
            .add_named_phase("Ensure both clients get back the result")
            .map_state(|mut state| {
                state.change_command(client1, Some(Command::Get(255)));
                state.change_command(client2, Some(Command::Get(255)));
                state.clear_response(client1);
                state
            })
            .add_named_phase_invariant("value does not change".to_string(), |s| {
                s.get_app().get_map().get(&255) == Some(&42)
            })
            .search(|s| {
                s.get_response(client1) == Some(CommandResponse::Value(42))
                    && s.get_response(client2) == Some(CommandResponse::Value(42))
            });

        assert!(
            res.results.is_found(),
            "Failed to finish the search. Got {:?}",
            res,
        );
    }

    #[test]
    fn two_servers_change_different_values() {
        // setup
        let mut addr_fact = AddressConstructor::new();

        let server = addr_fact.construct_address();
        let server_node = Server::new(MapApplication::new());

        let client1 = addr_fact.construct_address();
        let client1_node = Client::new(server, Some(Command::Store(1, 1)));
        let client2 = addr_fact.construct_address();
        let client2_node = Client::new(server, Some(Command::Store(2, 2)));

        let mut start = State::new();
        start.add_node(server, server_node);
        start.add_node(client1, client1_node);
        start.add_node(client2, client2_node);

        let mut base_searcher =
            SearchConfig::new(BloomCache::new(UnstableNetworkWrapper::new(start)));
        base_searcher.set_timeout(Duration::from_secs(5));

        let res = begin_multi_phase_search(base_searcher)
            .search(|s| {
                s.get_response(client1) == Some(CommandResponse::Ok())
                    && s.get_response(client2) == Some(CommandResponse::Ok())
                    && s.ctx.responses.len() == 0
                    && s.ctx.requests.len() == 0
            })
            .add_named_phase("Ensure both clients can see the others changes")
            .map_state(|mut state| {
                state.change_command(client1, Some(Command::Get(2)));
                state.change_command(client2, Some(Command::Get(1)));
                state.clear_response(client1);
                state.clear_response(client2);
                state
            })
            .add_named_phase_invariant("values do not change".to_string(), |s| {
                s.get_app()
                    .get_map()
                    .iter()
                    .all(|(k, v)| (k == &1 || k == &2) && k == v)
            })
            .search(|s| {
                s.get_response(client1) == Some(CommandResponse::Value(2))
                    && s.get_response(client2) == Some(CommandResponse::Value(1))
            });

        assert!(
            res.results.is_found(),
            "Failed to finish the search. Got {:?}",
            res,
        );
    }

    #[test]
    fn one_server_that_drops_both_messages() {
        // setup
        let mut addr_fact = AddressConstructor::new();

        let server = addr_fact.construct_address();
        let server_node = Server::new(MapApplication::new());

        let client1 = addr_fact.construct_address();
        let client1_node = Client::new(server, Some(Command::Store(1, 1)));

        let mut start = State::new();
        start.add_node(server, server_node);
        start.add_node(client1, client1_node);

        let mut base_searcher =
            SearchConfig::new(BloomCache::new(UnstableNetworkWrapper::new(start)));
        base_searcher.set_timeout(Duration::from_secs(5));

        let res = begin_multi_phase_search_with_name(
            base_searcher,
            "Finding state where the request was dropped",
        )
        .search(|s| {
            !s.get_app().get_map().contains_key(&1)
                && s.ctx.responses.len() == 0
                && s.ctx.requests.len() == 0
        })
        .add_named_phase("Finding state where the response was dropped")
        .search(|s| {
            s.get_app().get_map().contains_key(&1)
                && s.ctx.responses.len() == 0
                && s.ctx.requests.len() == 0
                && s.get_response(client1) == None
        })
        .add_named_phase("Finding state where the client gets the response")
        .add_named_phase_invariant(
            "Value should not change when re-running the command.".to_string(),
            |s| s.get_app().get_map().get(&1) == Some(&1),
        )
        .search(|s| s.get_response(client1) == Some(CommandResponse::Ok()));

        assert!(
            res.results.is_found(),
            "Failed to finish the search. Got {:?}",
            res,
        );
    }
}
